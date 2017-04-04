from __future__ import print_function

import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import dynamo_sessions
import common
import decimal

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


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
        if res.startswith('/messages/set-read'):
            user_id = int(path_p['userId'])
            msg_id = path_p['messageId']
            is_set = dynamo_sessions.set_message_read(user_id, msg_id)
            if is_set:
                return common.gen_json_resp({'success':True})
            else:
                return common.gen_json_resp({'success':False,
                                             'message':"not found"},
                                            code='404')
        else:
            start = parse_tstamp(qsp,'start')
            end = parse_tstamp(qsp,'end')
            user_id = int(path_p['userId'])
            msgs = dynamo_sessions.get_user_messages(user_id,start_t=start,end_t=end)
            msgs.sort(key=lambda x: x.get('created'))
            return common.gen_json_resp({'success':True,
                                         'messages':msgs})
    except:
        LOGGER.exception("Unable to handle request")
        return common.gen_json_resp({'success':False,
                                     'message':'error handling request'},
                                    code='500')
