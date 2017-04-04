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
    LOGGER.debug("Created session {0} for account {1}, user {3}, queue={2}".format(d['sessionId'],d['accountId'],d['userId'],d['sqsUrl']))
    return d

def delete_expired():
    # delete ones that expired more than 2 days ago
    # put in limit to ensure progress before potential timeout
    t = get_session_table()
    del_cnt = 0
    max_age = int(os.getenv('SESSION_INACTIVE_PURGE_SEC',86400))
    while True:
        q = {'ProjectionExpression': "userId, sessionId",
             'Limit':1000,
             'FilterExpression': Attr('expires').lt(int(time.time()-max_age))}
        sessions = collect_results(t.scan,q)
        for s in sessions:
            LOGGER.info("Deleting expired session, userId={0}, sessionId={1}".format(
                s['userId'],s['sessionId']))
            t.delete_item(Key={'userId':s['userId'],
                               'sessionId':s['sessionId']})
            del_cnt += 1
        if len(sessions)<1000:
            break
    return del_cnt

def destroy(account_id, user_id, session_id):
    get_session_table().delete_item(Key={'userId':user_id,
                                         'sessionId':session_id})

def lookup(account_id=None, user_id=None, session_id=None, max_expired_age=None):
    q = {'Select': 'ALL_ATTRIBUTES'}
    if user_id is not None:
        q['KeyConditionExpression'] = Key('userId').eq(user_id)
        if session_id is not None:
            q['KeyConditionExpression'] = q['KeyConditionExpression'] & Key('sessionId').eq(session_id)
        if account_id is not None:
            q['FilterExpression'] = Attr('accountId').eq(account_id)
    elif account_id is not None:
        # use the account GSI
        q['KeyConditionExpression'] = Key('accountId').eq(account_id)
        q['IndexName'] = os.getenv('SESSION_TABLE_ACCOUNT_GSI')
        if session_id is not None:
            q['FilterExpression'] = Attr('sessionId').eq(session_id)
    elif session_id is not None:
        q['FilterExpression'] = Attr('sessionId').eq(session_id)
    else:
        return get_all_sessions(max_expired_age=max_expired_age)

    if max_expired_age is not None:
        exp_filter = Attr('expires').gte(int(time.time()-max_expired_age))
        if 'FilterExpression' in q:
            q['FilterExpression'] = q['FilterExpression'] & exp_filter
        else:
            q['FilterExpression'] = exp_filter

    if 'KeyConditionExpression' in q:
        return collect_results(get_session_table().query,q)
    else:
        return collect_results(get_session_table().scan,q)


def get_all_sessions(max_expired_age=None):
    q = {'Select': 'ALL_ATTRIBUTES'}
    if max_expired_age is not None:
         q['FilterExpression'] = Attr('expires').gte(int(time.time()-max_expired_age))
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

