from __future__ import print_function

import os
import sys
import time
import boto3
import json

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import dynamo_sessions
import common

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)



def lambda_handler(event, context):
    # create list of user_id,msg
    if 'user_msg_list' in event:
        user_msg_list = event['user_msg_list']
        msg_list = []
        msg_ttl = int(time.time())+int(os.getenv('USER_MESSAGE_TTL'))
        for user_id,msgs in user_msg_list:
            for m in msgs:
                m['is_read'] = 0
                m['ttl'] = msg_ttl
                msg_list.append( (user_id,m) )
        dynamo_sessions.batch_add_user_history(msg_list,n_workers=50)
        LOGGER.info("Added {0} entries to user history".format(len(msg_list)))
    elif 'user_msg_read_receipts' in event:
        user_id = int(event['user_id'])
        msg_id_list = event['user_msg_read_receipts']
        LOGGER.info("Setting {0} messages read for user_id {1}".format(len(msg_id_list),user_id))
        receipted_msg_ids = dynamo_sessions.set_messages_read(user_id,msg_id_list)
        if os.getenv('SEND_READ_RECEIPTS_VIA_SQS','1').lower().strip() in ('1','true','yes'):
            # send msg-receited updates to any SQS queues for the user
            LOGGER.info("Generating read-receipt message for user_id={0}".format(user_id))
            m = {'userId':user_id,
                 '_type':'message-read-receipt',
                 'messages-receipted': receipted_msg_ids,
                 '_sqs_only': 1}
            try:
                c = boto3.client('lambda')
                c.invoke(FunctionName=os.getenv('DISPATCHER_LAMBDA'),
                         Payload=json.dumps({'Records':[m]},cls=common.DecimalEncoder),
                         InvocationType='Event')
            except:
                LOGGER.exception("Unable to push read-receipt message for user_id={0}".format(user_id))
