from __future__ import print_function

import sys
import os
import pprint
import urllib
import random
import uuid
import time
import json
import decimal

import boto3
import unittest
import testenv

import requests
from requests_aws4auth import AWS4Auth

from common import get_msgs




class TestSessions(unittest.TestCase):
    
    def setUp(self):
        self.props = testenv.setup()
        auth = AWS4Auth(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'),
                        self.props['REGION'],
                        'execute-api')
        def call_gateway(path,params=None):
            url = self.props['GATEWAY_URL'] + path
            print("calling {0} : {1!r}".format(url,params))
            resp = requests.get(url,params=params,auth=auth)
            d = json.loads(resp.text)
            return d
        self.call_gw = call_gateway

    
    def test_high_vol(self):
        # create 50 listeners
        accounts = [ (random.randint(10000000,10000010),
                      str(uuid.uuid1()),
                      random.randint(80000001,90000000))
                     for _ in xrange(50) ]
        s_list = [ self.call_gw('/create/{0}/{1}/{2}'.format(ac_id,user_id,s)) for ac_id,s,user_id in accounts ]
        
        c = boto3.client('kinesis',region_name=self.props['REGION'])

        msg_list = []
        stream = self.props['EVENT_STREAM']
        # create 100 broadcast messages
        for _ in xrange(100):
            msg_list.append({'StreamName':stream,
                             'Data':json.dumps({'msg':str(random.randint(0,1000000000))}),
                             'PartitionKey':str(random.randint(0,1000000000))})
        # for each account, create three user messages and one account message
        for ac_id,s,user_id in accounts:
            msg_list.append({'StreamName':stream,
                             'Data':json.dumps({'msg':str(random.randint(0,1000000000)),
                                                'accountId':ac_id}),
                             'PartitionKey':str(random.randint(0,1000000000))})
            msg_list.append({'StreamName':stream,
                             'Data':json.dumps({'msg':str(random.randint(0,1000000000)),
                                                'accountId':ac_id,
                                                'userId':user_id}),
                             'PartitionKey':str(random.randint(0,1000000000))})
            msg_list.append({'StreamName':stream,
                             'Data':json.dumps({'msg':str(random.randint(0,1000000000)),
                                                'accountId':ac_id,
                                                'userId':user_id}),
                             'PartitionKey':str(random.randint(0,1000000000))})

        # shuffle messages
        random.shuffle(msg_list)
        for m in msg_list:
            c.put_record(**m)
        print("Messages posted, waiting for receipt")
        time.sleep(60)

        # pull one of the queues
        s = s_list[0]['session']
        act_id = s['accountId']
        user_id = s['userId']
        
        queue_items = get_msgs(s)

        # what should be in there?
        msg_d_list = [ json.loads(x['Data']) for x in msg_list ]
        s_msg_list = [ x['msg'] for x in msg_d_list
                       if x.get('accountId') in (None,act_id) or x.get('userId')==user_id ]
        s_msg_list.sort()

        json.dump(queue_items,open('msg_list','w'))
        json.dump(s_msg_list,open('s_msg_list','w'))
        self.assertListEqual(queue_items, s_msg_list)
if __name__=="__main__":
    unittest.main()
