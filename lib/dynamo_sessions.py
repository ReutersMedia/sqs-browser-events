import os
import time
import decimal
import concurrent.futures
import threading

import boto3

from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeSerializer
import botocore.exceptions

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

_thread_local = threading.local()

def get_session():
    if not hasattr(_thread_local,'boto_session'):
        _thread_local.boto_session = boto3.session.Session()
    return _thread_local.boto_session


import common

import cachetools.func


def get_session_table():
    dynamodb = boto3.resource('dynamodb')
    return dynamodb.Table(os.getenv('SESSION_TABLE'))

def get_history_table(new_session=False):
    if new_session:
        session = get_session()
        dynamodb = session.resource('dynamodb')
    else:
        dynamodb = boto3.resource('dynamodb')
    return dynamodb.Table(os.getenv('HISTORY_TABLE'))

def quantize_tstamp(ts):
    return ts.quantize(decimal.Decimal('0.000001'),rounding=decimal.ROUND_HALF_UP)

def set_message_read(user_id, msg_id):
    try:
        r=get_history_table(new_session=True).update_item(
            Key={'userId':user_id,
                 'messageId':msg_id},
            UpdateExpression="set is_read = :a",
            ExpressionAttributeValues={':a': 1},
            ConditionExpression="is_read <> :a")
        LOGGER.info("Read-receipted user_id={0} message_id={1}".format(user_id,msg_id))
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            LOGGER.info("Message already read user_id={0}, msg_id={1}".format(user_id,msg_id))
            return False
        else:
            LOGGER.exception("Eror updating read setting for user_id={0}, msg_id={1}".format(user_id,msg_id))
            return False

def set_messages_read(user_id, msg_id_list, n_workers=30):
    # as it's an update, can't use batch-write
    LOGGER.info("read-receipting {0} messages for user_id={1}".format(len(msg_id_list),user_id))
    msg_update_status = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
        future_to_msg_id = {executor.submit(set_message_read, user_id, msg_id): msg_id for msg_id in msg_id_list}
        for future in concurrent.futures.as_completed(future_to_msg_id):
            msg_id = future_to_msg_id[future]
            msg_update_status[msg_id] = future.result()
    return [k for k,v in msg_update_status.iteritems() if v]

def get_unread_message_ids_asof(user_id, asof):
    # get list of messageIds
    q = {'KeyConditionExpression': Key('userId').eq(user_id),
         'FilterExpression': Attr('created').lte(asof) & Attr('is_read').ne(1),
         'Limit':1000,
         'Select':'SPECIFIC_ATTRIBUTES',
         'ProjectionExpression':'messageId'}
    r = collect_results(get_history_table().query,q)
    msg_ids = [x['messageId'] for x in r]
    return msg_ids
        
def write_user_history(item_batch):
    # use a consistent timestamp (tnow) so that any reprocessing results in overwriting if
    # items are inserted multiple times
    try:
        hist_table = os.getenv('HISTORY_TABLE')
        session = get_session()
        c = session.client('dynamodb')
        r = c.batch_write_item(RequestItems={ hist_table: item_batch })
        unproc = r.get('UnprocessedItems')
        if unproc is not None and hist_table in unproc and len(unproc[hist_table])>0:
            return unproc[hist_table]
        return []
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ValidationException':
            LOGGER.error("Validation exception writing: {0!r}".format(item_batch))
        else:
            LOGGER.exception("DynamoDB Client error")
        return item_batch
    except:
        LOGGER.exception("Error inserting user batch")
        # assume all failed
        return item_batch

def trim_empty_leafs(d):
    for k,v in d.iteritems():
        if isinstance(v,dict):
            v = dict(trim_empty_leafs(v))
            if len(v)>0:
                yield (k,v)
        elif isinstance(v,str) or isinstance(v,unicode):
            if len(v)>0:
                yield (k,v)
        elif v is not None:
            yield (k,v)
    
def convert_to_dyn_objects(user_msg_list,tnow):
    tnow_dec = quantize_tstamp(decimal.Decimal(tnow))
    ts = TypeSerializer()
    msg_d = {}
    def build_item(user_id,msg):
        # hash of message and timestamp
        item = msg.copy()
        item['userId'] = user_id
        item['created'] = tnow_dec
        item = common.floats_to_decimals(item)
        # filter out item values
        item_dyn = dict([(k,ts.serialize(v)) for k,v in item.iteritems()])
        item_dyn = dict(trim_empty_leafs(item_dyn))
        return ((item['userId'],item['messageId']),{'PutRequest':{'Item':item_dyn}})
    # adding to dict will ensure only latest message kept for each user_id/message_id
    for user_id,m in user_msg_list:
        key,dyn_item = build_item(user_id,m)
        msg_d[key] = dyn_item
    return msg_d.values()
    
    
def batch_add_user_history(user_msg_list,n_workers=25):
    try_cnt = 0
    tnow = time.time()
    user_msg_list = convert_to_dyn_objects(user_msg_list,tnow)
    failures = []
    while len(user_msg_list)>0 and try_cnt <= 5:
        try_cnt += 1
        # split into batches of 25
        failures = []
        batches = [user_msg_list[i:i+25] for i in xrange(0,len(user_msg_list),25)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            future_to_userbatch = {executor.submit(write_user_history, b): b for b in batches}
            for future in concurrent.futures.as_completed(future_to_userbatch):
                user_batch = future_to_userbatch[future]
                failed_items = future.result()
                failures.extend(failed_items)
                LOGGER.debug("User batch write: {0} failures".format(len(failed_items)))
        if len(failures)>0:
            time.sleep(try_cnt*5)
        user_msg_list = failures
    if len(failures)>0:
        LOGGER.error("Failure sending user batch writes, dropped {0}".format(len(failures)))
    LOGGER.info("Done adding to user history")

def get_user_messages(user_id,start_t=None,end_t=None,type_filter=None):
    q = {'KeyConditionExpression': Key('userId').eq(user_id)} 
    if start_t is not None and end_t is not None:
        q['FilterExpression'] = Attr('created').gte(start_t) & Attr('created').lte(end_t)
    elif start_t is not None:
        q['FilterExpression'] = Attr('created').gte(start_t)
    elif end_t is not None:
        q['FilterExpression'] = Attr('created').lte(end_t)
    if type_filter is not None:
        type_exp = Attr('_type').is_in(type_filter)
        if 'FilterExpression' in q:
            q['FilterExpression'] = q['FilterExpression'] & type_exp
        else:
            q['FilterExpression'] = type_exp
    return collect_results(get_history_table().query,q)
        
def create(d):
    get_session_table().put_item(Item=d)
    LOGGER.debug("Created session {0} for account {1}, user {3}, queue={2}".format(d['sessionId'],d['accountId'],d['userId'],d['sqsUrl']))
    return 


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

# expensive call, so cache briefly if we have a flood of broadcast messages
@cachetools.func.ttl_cache(maxsize=10,ttl=30)
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

