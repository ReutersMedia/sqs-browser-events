from __future__ import print_function

import os
import sys
import logging
import boto3

def setup():
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)
    sls_env = os.getenv('SLS_ENV')
    if sls_env is None:
        print("serverless environment name must be provided via SLS_ENV environment variable")
        sys.exit(1)
    sls_region = os.getenv('SLS_REGION')
    if sls_region is None: 
        print("serverless AWS region name must be provided via SLS_REGION environment variable")
        sys.exit(1)
    os.environ['AWS_DEFAULT_REGION'] = sls_region
    # lookup API gateway
    client = boto3.client('apigateway',region_name=sls_region)
    r = client.get_rest_apis(limit=1000)
    gw_id = [x['id'] for x in r['items'] if x['name']=="{0}-sqsbrowserevents".format(sls_env)][0]
    gw_url = "https://{0}.execute-api.{1}.amazonaws.com/{2}".format(gw_id,sls_region,sls_env)
    # set resource ARNs
    return {
        'SQS_QUEUE_PREFIX': sls_env+"-brws-",
        'SESSION_TABLE': sls_env+"-sqs-browser-sessions",
        'EVENT_STREAM': sls_env+"-sqs-browser-event-stream",
        'REGION': sls_region,
        'IDENTITY_POOL': 'sqs_browser',
        'GATEWAY_URL': gw_url }
