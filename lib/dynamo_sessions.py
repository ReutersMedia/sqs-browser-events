import os
import time
import decimal

import boto3

from boto3.dynamodb.conditions import Key, Attr
import botocore.exceptions

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def get_session_table():
    dynamodb = boto3.resource('dynamodb')
    return dynamodb.Table(os.getenv('SESSION_TABLE'))

def create(d):
    get_session_table().put_item(Item=d)
    LOGGER.debug("Created session {0} for account {1}, queue={2}".format(d['sessionId'],d['accountId'],d['sqsUrl']))
    return d

def delete_expired():
    # delete ones that expired more than 2 days ago
    # put in limit to ensure progress before potential timeout
    t = get_session_table()
    del_cnt = 0
    while True:
        q = {'ProjectionExpression': "accountId, sessionId",
             'Limit':1000,
             'FilterExpression': Attr('expires').lt(int(time.time())-86400*2)}
        sessions = collect_results(t.scan,q)
        for s in sessions:
            LOGGER.info("Deleting expired session, accountId={0}, sessionId={1}".format(
                s['accountId'],s['sessionId']))
            t.delete_item(Key={'accountId':s['accountId'],
                               'sessionId':s['sessionId']})
            del_cnt += 1
        if len(sessions)<1000:
            break
    return del_cnt

def destroy(account_id, session_id):
    get_session_table().delete_item(Key={'accountId':account_id,
                                         'sessionId':session_id})

def lookup(account_id, user_id=None, session_id=None):
    expires_filter = Attr('expires').gte(int(time.time()))
    q = {'Select': 'ALL_ATTRIBUTES',
         'FilterExpression': expires_filter}
    if session_id is None:
        q['KeyConditionExpression'] = Key('accountId').eq(account_id)
        if user_id is not None:
            q['FilterExpression'] = Attr('userId').eq(int(user_id)) & expires_filter
    else:
        q['KeyConditionExpression'] = Key('accountId').eq(account_id) & Key('sessionId').eq(session_id)
    return collect_results(get_session_table().query,q)


def get_all_sessions():
    q = {'Select': 'ALL_ATTRIBUTES',
         'FilterExpression': Attr('expires').gte(int(time.time()))}
    return collect_results(get_session_table().scan,q)

def get_all_sqs_urls():
    q = {'Select': 'SPECIFIC_ATTRIBUTES',
         'AttributesToGet': ['sqsUrl']}
    items = collect_results(get_session_table().scan,q)
    return [x['sqsUrl'] for x in items]


def collect_results(table_f,qp):
    items = []
    while True:
        r = table_f(**qp)
        items.extend(r['Items'])
        lek = r.get('LastEvaluatedKey')
        if lek is None or lek=='':
            break
        qp['ExclusiveStartKey'] = lek
    return items

