from __future__ import print_function

import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import dynamo_sessions

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    # create list of user_id,msg
    user_msg_list = event['user_msg_list']
    msg_list = []
    for user_id,msgs in user_msg_list:
        for m in msgs:
            m['is_read'] = 0
            msg_list.append( (user_id,m) )
    dynamo_sessions.batch_add_user_history(msg_list,n_workers=50)
    LOGGER.info("Added {0} entries to user history".format(len(msg_list)))
    return True
