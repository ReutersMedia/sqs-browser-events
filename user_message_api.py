from __future__ import print_function

import os
import sys
import base64
import json

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import dynamo_sessions
import common
import decimal
import boto3

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


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
                msg_status_d = dynamo_sessions.set_messages_read(user_id, msg_id_list)
            elif 'tstamp' in path_p:
                asof = int(float(path_p['tstamp']))
                msg_status_d = dynamo_sessions.set_messages_read_asof(user_id, asof)
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
                msg_status_d = dynamo_sessions.set_messages_read(user_id, msg_id_list)
            # send msg-receited updates to any SQS queues for the user
            if os.getenv('SEND_READ_RECEIPTS_VIA_SQS','').lower() in ('1','true','yes'):
                LOGGER.info("Generating read-receipt message for user_id={0}".format(user_id))
                m = {'userId':user_id,
                     '_type':'message-read-receipt',
                     'messages-receipted': msg_status_d,
                     '_sqs_only': 1}
                try:
                    c = boto3.client('kinesis')
                    c.put_record(StreamName=os.getenv('EVENT_STREAM'),
                                 Data=json.dumps(m,cls=common.DecimalEncoder),
                                 PartitionKey=str(user_id))
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
