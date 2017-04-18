from __future__ import print_function

import hashlib
import base64
import time
import os
import sys
import json

import boto3
import botocore.errorfactory

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import common
import dynamo_sessions

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class SessionManagerException(Exception):
    pass

def get_cognito_client():
    cog_region = os.getenv('COGNITO_REGION')
    if cog_region is not None and len(cog_region)>0:
        return boto3.client('cognito-identity',region_name=cog_region)
    else:
        return boto3.client('cognito-identity')

        
_identity_pool_id = None
def get_identity_pool():
    global _identity_pool_id
    if _identity_pool_id==None:
        c = get_cognito_client()
        pools = c.list_identity_pools(MaxResults=60)['IdentityPools']
        pool_name = os.getenv('COGNITO_IDENTITY_POOL')
        try:
            m = next(x for x in pools if x['IdentityPoolName']==pool_name)
        except StopIteration:
            raise SessionManagerException("Unable to find identity pool with name {0}".format(pool_name))
        _identity_pool_id = m['IdentityPoolId']
        LOGGER.info("found IdentityPoolId {0}".format(_identity_pool_id))
    return _identity_pool_id

def parse_id(path_p,field):
    try:
        return int(path_p[field])
    except:
        LOGGER.exception("must provide an integer {0}".format(field))
        raise SessionManagerException("Invalid {0}, must be integer".format(field))

def get_credentials(cog_client, cog_id):
    r = cog_client.get_credentials_for_identity(IdentityId=cog_id)
    expires = int(time.mktime(r['Credentials']['Expiration'].utctimetuple()))
    session_ttl = expires + int(os.getenv('SESSION_TTL'))
    return {'accessKey': r['Credentials']['AccessKeyId'],
            'secretKey': r['Credentials']['SecretKey'],
            'sessionToken': r['Credentials']['SessionToken'],
            'expires': expires,
            'ttl': session_ttl}
    
def create_sqs_queue(account_id, user_id, session_id, restrict_ip=None):
    # first lookup to see if we already have for this session
    d = dynamo_sessions.lookup(account_id, user_id=user_id, session_id=session_id, max_expired_age=86400)
    cog_c = get_cognito_client()
    if len(d)>= 1:
        LOGGER.info("Session {0} already exists, reusing".format(session_id))
        # if credentials expire in less than an hour, then renew
        session = d[0]
        if int(session['expires'])-int(time.time()) < 3600:
            # renew
            creds = get_credentials(cog_c,session['identityId'])
            session.update(creds)
        return session
    sqs_c = boto3.client('sqs')
    pool_id = get_identity_pool()
    r = cog_c.get_id(IdentityPoolId=pool_id)
    cog_id = r['IdentityId']
    cred_d = get_credentials(cog_c,cog_id)
    if restrict_ip is not None:
        policy = {'Version': '2012-10-17',
                  'Statement':[{'Effect': "Allow",
                                'Action': [
                                    "sqs:ReceiveMessage",
                                    "sqs:GetQueueAttributes"
                                ],
                                'Condition': {
                                    'IpAddress': {
                                        "aws:SourceIp":restrict_ip}
                                },
                                'Resource': "*"}
                  ]}
    else:
        policy = None
    # use hash to generate queue name based on account, session
    # this ensures it is well-distributed, which will be useful when
    # we need to scroll through a large number of queues
    queue_name = os.getenv('SQS_QUEUE_PREFIX') + \
                 base64.urlsafe_b64encode(hashlib.sha1("{0}-{1}".format(account_id,session_id)).digest()).replace('=','')
    # unix /dev/random has more entropy than /dev/urandom, and python random
    with open('/dev/random','rb') as f_rand:
        aes_key = f_rand.read(32)
    q_attr = {'MessageRetentionPeriod':str(int(os.getenv('SQS_MESSAGE_RETENTION_PERIOD')))}
    if policy is not None:
        q_attr['Policy'] = json.dumps(policy)
    try:
        r = sqs_c.create_queue(QueueName=queue_name,
                               Attributes=q_attr)
    except botocore.errorfactory.ClientError, err:
        if err.response['Error']['Code']=='QueueAlreadyExists' and policy is not None:
            LOGGER.info("Queue {0} already exists, resetting policy".format(queue_name))
            r = sqs_c.get_queue_url(QueueName=queue_name)
            sqs_c.set_queue_attributes(QueueUrl=r['QueueUrl'],
                                       Attributes={'Policy':json.dumps(policy)})
    queue_url = r['QueueUrl']
    # create an encryption key
    
    m = {'accountId':account_id,
         'userId':user_id,
         'sessionId':session_id,
         'sqsUrl':queue_url,
         'sqsQueueName':queue_name,
         'identityId':cog_id,
         'aesKey':base64.b64encode(aes_key)}
    m.update(cred_d)
    dynamo_sessions.create(m)
    return m


def destroy_session(account_id, user_id, session_id):
    d = dynamo_sessions.lookup(account_id, user_id=user_id, session_id=session_id)
    if len(d)>0:
        sqs_url = d[0]['sqsUrl']
        c = boto3.client('sqs')
        c.delete_queue(QueueUrl=sqs_url)
        LOGGER.info("Removed queue {0}".format(sqs_url))
        LOGGER.info("Destroying session {0}, user {1}".format(session_id,user_id))
        dynamo_sessions.destroy(account_id, user_id, session_id)
        return {"success":True}
    else:
        return {"success":False,
                "message":"session not found"}

def get_all_sessions():
    return {"success":True,
            "sessions": dynamo_sessions.get_all_sessions()}


def get_session_status(account_id, user_id, session_id):
    if session_id is None:
        raise SessionManagerException("Destroy action must include a sessionId")
    d = dynamo_sessions.lookup(account_id, user_id=user_id, session_id=session_id)
    if len(d)>0:
        return {"success":True,
                "session":d[0]}
    else:
        return {"success":True,
                "session":None}
    

def renew_session(account_id, user_id, session_id):
    m = dynamo_sessions.lookup(account_id, user_id=user_id, session_id=session_id)
    if len(m)==0:
        return {"success":False,
                "message":"Session has expired"}
    m = m[0]
    c = get_cognito_client()
    cred_d = get_credentials(c,m['identityId'])
    m.update(cred_d)
    dynamo_sessions.create(m)
    LOGGER.info("renewed session for account_id={0}, session_id={1}".format(account_id,session_id))
    return {"success":True,
            "session":m}
    
    
def create_session(account_id, user_id, session_id, restrict_ip=None):
    if len(session_id)>256:
        raise SessionManagerException("sessionId can not be longer than 256 characters")
    m = create_sqs_queue(account_id, user_id, session_id, restrict_ip=restrict_ip)
    LOGGER.info("created session for account_id={0}, session_id={1}, user_id={2}".format(account_id,session_id,user_id))
    return {"success":True,
            "session":m}

    
SQS_NAME_CHARS = [ chr(x) for x in range(97,123)+range(65,91)+range(48,58) ] + ['-','_']

def cleanup_queues():
    prefix = os.getenv('SQS_QUEUE_PREFIX')
    if len(prefix)==0:
        LOGGER.error("Queue name prefix missing, refusing to delete")
        return
    db_queues = dynamo_sessions.get_all_sqs_urls()
    # SQS list_queues is limited to 1000, so may need to split up the queries.
    # we only need to split up if db_queues is very long.
    # if db_queues is short and actual number of queues is > 1000, we will delete
    # most and eventually make headway
    if len(db_queues)>20000:
        # take first two characters
        prefixes = [ prefix+i+j for i in SQS_NAME_CHARS for j in SQS_NAME_CHARS ]
    elif len(db_queues)>500:
        # take first character
        prefixes = [ prefix+i for i in SQS_NAME_CHARS ]
    else:
        prefixes = [ prefix ]
    return sum([remove_unused_queues(x,db_queues) for x in prefixes])


def remove_unused_queues(sqs_name_prefix,db_queues):
    db_queues = set(db_queues)
    c = boto3.client('sqs')
    if sqs_name_prefix is None:
        r = c.list_queues()
    else:
        r = c.list_queues(QueueNamePrefix=sqs_name_prefix)
    aws_queues = r.get('QueueUrls')
    if aws_queues == None:
        LOGGER.info("No queues to purge")
        return
    # delete ones in aws_queues that aren't in db_queues
    def del_q(x):
        LOGGER.info("Deleting queue {0}".format(x))
        try:
            c.delete_queue(QueueUrl=q)
            return 1
        except:
            LOGGER.exception("Error removing queue {0}".format(x))
            return 0
    return sum([ del_q(q) for q in aws_queues if q not in db_queues ])

def gen_json_resp(d, code='200'):
    return {'statusCode': code,
            'body': json.dumps(d,cls=common.DecimalEncoder),
            'headers': {
                'Content-Type': 'application/json'
                }
        }

def api_gateway_handler(event, context):
    LOGGER.debug("Received event: {0!r}".format(event))
    path_p = event.get('pathParameters')
    qsp = event.get('queryStringParameters')
    if qsp is None:
        qsp = {}
    try:
        res = event['resource']
        if res.startswith('/create'):
            r = create_session(parse_id(path_p,'accountId'),
                               parse_id(path_p,'userId'),
                               path_p['sessionId'])
        elif res.startswith('/destroy'):
            r = destroy_session(parse_id(path_p,'accountId'),
                                parse_id(path_p,'userId'),
                                path_p['sessionId'])
        elif res.startswith('/renew'):
            r = renew_session(parse_id(path_p,'accountId'),
                              parse_id(path_p,'userId'),
                              path_p['sessionId'])
        elif res.startswith('/status'):
            if path_p is None:
                r = get_all_sessions()
            else:
                r = get_session_status(parse_id(path_p,'accountId'),
                                       parse_id(path_p,'userId'),
                                       parse_p['sessionId'])
        elif res.startswith('/cleanup'):
            return {'queues_removed':cleanup_queues(),
                    'success':True}
        else:
            raise SessionManagerException("Unrecognized action")
        return gen_json_resp(r)
    except:
        LOGGER.exception('Error handling event {0!r}'.format(event))
        return gen_json_resp({'success':False,
                              'message':'error handling request'},
                             code='500')
    
def cleanup_lambda_handler(event, context):
    LOGGER.info("Cleanup handler called")
    cleanup_queues()
