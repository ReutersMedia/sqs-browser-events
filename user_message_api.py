from __future__ import print_function

import os
import sys
import base64
import json
import threading

from multiprocessing.pool import ThreadPool

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import dynamo_sessions
import common
import decimal
import boto3

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


_thread_local = threading.local()

def get_session():
    if not hasattr(_thread_local,'boto_session'):
        _thread_local.boto_session = boto3.session.Session()
    return _thread_local.boto_session


class RequestFormatException(Exception):
    pass

def parse_tstamp(qsp,field):
    if field in qsp:
        try:
            return int(decimal.Decimal(qsp[field]))
        except:
            LOGGER.error("Invalid {0} parameter: {1}.  Must be integer.".format(field,qsp[field]))
            raise
    else:
        return None


def send_read_receipt_event(user_id,msg_id_list):
    try:
        session = get_session()
        c = session.client('lambda')
        c.invoke(FunctionName=os.getenv('USER_HISTORY_ADDER_LAMBDA'),
                 Payload=json.dumps({'user_id':user_id,
                                     'user_msg_read_receipts':msg_id_list}),
                 InvocationType='Event')
        LOGGER.info("Dispatched {0} read receipts for user {1}".format(len(msg_id_list),user_id))
    except:
        LOGGER.exception("Unable to create user read receipt event")

    
def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i + n]
    
def create_msg_read_receipt_events(user_id,msg_id_list):
    if len(msg_id_list)==0:
        return
    chunk_size = int(os.getenv('READ_RECEIPT_ADDER_CHUNK_SIZE',50))
    msg_batches = chunks(msg_id_list,chunk_size)
    m_tp = ThreadPool(min(20,1+len(msg_id_list)/chunk_size))
    m_tp.map(lambda x: send_read_receipt_event(user_id,x),msg_batches)
    m_tp.close()
    
def api_gateway_handler(event, context):
    LOGGER.debug("Received event: {0!r}".format(event))
    path_p = event.get('pathParameters')
    qsp = event.get('queryStringParameters')
    if qsp is None:
        qsp = {}
    try:
        res = event['resource']
        if res.startswith('/messages/set-read/user'):
            user_id = int(path_p['userId'])
            if 'messageId' in path_p:
                msg_id_list = path_p['messageId'].split(',')
            elif 'tstamp' in path_p:
                asof = int(float(path_p['tstamp']))
                msg_id_list = dynamo_sessions.get_unread_message_ids_asof(user_id,asof)
            else: # POST with JSON
                body = event.get('body')
                if event.get('isBase64Encoded'):
                    body = base64.b64decode(body)
                try:
                    req_data = json.loads(body)
                except:
                    LOGGER.exception("Error parsing body: {0!r}".format(body))
                    raise RequestFormatException("Request body must be JSON")
                # req_data should be a list of numbers
                if not isinstance(req_data,list):
                    raise RequestFormatException("Request body must be a list of message IDs")
                msg_id_list = [str(x) for x in req_data]
            if 'async' in qsp:
                create_msg_read_receipt_events(user_id,msg_id_list)
                return common.gen_json_resp({'success':True})
            else:
                msg_status_d = dynamo_sessions.set_messages_read(user_id, msg_id_list)
                # send msg-receited updates to any SQS queues for the user
                if os.getenv('SEND_READ_RECEIPTS_VIA_SQS','1').lower().strip() in ('1','true','yes'):
                    LOGGER.info("Generating read-receipt message for user_id={0}".format(user_id))
                    m = {'userId':user_id,
                         '_type':'message-read-receipt',
                         'messages-receipted': msg_status_d,
                         '_sqs_only': 1}
                    try:
                        c = boto3.client('lambda')
                        c.invoke(FunctionName=os.getenv('DISPATCHER_LAMBDA'),
                                 Payload=json.dumps({'Records':[m]},cls=common.DecimalEncoder),
                                 InvocationType='Event')
                    except:
                        LOGGER.exception("Unable to push read-receipt message for user_id={0}".format(user_id))
                return common.gen_json_resp({'success':True,
                                             'messages_receipted': msg_status_d})
        else:
            start = parse_tstamp(qsp,'start')
            end = parse_tstamp(qsp,'end')
            # type filter
            _type = qsp.get('_type')
            if _type is not None:
                _type = [x.strip() for x in _type.split(',')]
            user_id = int(path_p['userId'])
            msgs = dynamo_sessions.get_user_messages(user_id,start_t=start,end_t=end,type_filter=_type)
            msgs.sort(key=lambda x: x.get('created'),reverse=True)
            return common.gen_json_resp({'success':True,
                                         'messages':msgs})
    except RequestFormatException, rfex:
        LOGGER.error("Invalid request format")
        return common.gen_json_resp({'success':False,
                                     'message':rfex.message},
                                    code='400')
    except:
        LOGGER.exception("Unable to handle request")
        return common.gen_json_resp({'success':False,
                                     'message':'error handling request'},
                                    code='500')
