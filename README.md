
## Overview

One challenge with serverless architectures is supporting event-driven browser experiences.  This package provides a solution by using AWS SQS and Lambda functions, and Cognito for creating temporary unauthorized sessions.  Typical use would be a backend system that creates a new session upon login, and passes an SQS Queue URL and a pair of AWS auth keys to the browser, and the browser then polls SQS for events.  A dispatcher API and Kinesis input stream is used to generate and dispatch events to the queues.


## Principles

Your backend application should generally create the session for the user and share SQS access information, and then generate messages as needed.

```
 [Application]
    /create
        -> cognito - get_id
        -> cognito - get_credentials
        -> sqs - create_queue, attaching a per-queue policy permissioning the cognito user
    send sqsUrl, credentials, and expiration time to brower

 [Browser]
    sqs - long-poll for new messages

 [Message Generator]
    /notify
    kinesis
```

For the `/create` method, you should supply an account ID and a session ID.  The account ID is used to partition the DynamoDB queue table, and support broadcast of messages to all logged in users for an account.  The session ID should be unique across all login sessions, as each client needs their own SQS queue.  The Session ID should be a string no longer than 256 characters.

The Cognito credentials expire after serveral hours, and periodically need to be renewed.  The client therefore needs to know the expiration time and request a refresh of the credentials.  The Cognito user also expires after several days without use.  The SQS Queues however need to be managed.  To cleanup unused Queues, a cleanup procedure is called every 6 hours via a Lambda cron, or can be called using the `/cleanup` API Gateway method.  The cleanup procedure will look for SQS queues that are no longer referenced in the DyanmoDB session table, and delete them.

Note that messages may accumulate while a user is not active, if they return and the same Session ID is used.


## Message Encryption

All messages are encrypted with AES using the counter mode of operation.  You will need to base-64 decode the AES key returned by the session creation call, and parse the SQS Body into a counter initialization value, and a message payload, as shown below.  Split the SQS message on the first pipe '|'.

```
    init_ctr,enc_msg = m['Body'].split('|',1)
    ctr = pyaes.Counter(initial_value=int(init_ctr))
    aes = pyaes.AESModeOfOperationCTR(base64.b64decode(session['aesKey']),counter=ctr)
    msg = aes.decrypt(base64.b64decode(enc_msg))
```

## Creating a Cognito Identity Pool

A stack needs a Cognito user pool, and this cannot be set up via Cloud Formation.  One identity pool however can be shared by a number of stacks, and the serverless.yml file requires a pool with the name "sqs_browser".  This can be overridden with a "--poolname" argument. 

To create the pool, log into the AWS console, and select the Cognito service.  Click on "Manage Federated Identities".  Then select "Create a new identity pool".  For the identity pool name, use "sqs_browser".  Under "Unauthenticated identities" select "Enable access to unauthenticated identities".  Then click "Create Pool".  This will allow the application to create users as needed, without using a 3rd party to authenticate them.

Then create a new IAM role, with the trust relationship given below, using the Pool ID from the above Cognito pool.  Associate this role with unauthenticated users via the console.

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "cognito-identity.amazonaws.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "cognito-identity.amazonaws.com:aud": "<COGNITO POOL ID>"
        },
        "ForAnyValue:StringLike": {
          "cognito-identity.amazonaws.com:amr": "unauthenticated"
        }
      }
    }
  ]
}
```

And embed the following inline policy, which allows access to all queues with the given name prefix.  Note that queue message privacy is based on encryption of the queue messages, and use of a cyrptographic hash to generate queue URLs.

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:GetQueueAttributes",
                "sqs:DeleteMessage",
                "sqs:DeleteMessageBatch",
                "sqs:PurgeQueue"
            ],
            "Resource": "arn:aws:sqs:*:*:cognito-sqs-*"
        }
    ]
}
```


## Session Manager API

In the API calls below, the accountId must be an integer, and sessionId must be a string with length less than 256 characters.  For the create, renew, and status endpoints, the response will contain session descriptions of the form given below.  The expiration time is for the access key and will be several hours in the future.  These access keys can be renewed periodically by calling the `renew` endpoint.

```
   {
      "sqsUrl": "https://eu-west-1.queue.amazonaws.com/XXXXXX/kentest1-brws-KLK0dJ6AN9USg6CAx8FXVk52G2M",
      "sqsQueueName": "kentest1-brws-XXXXXXX",
      "accessKey": "XXXX",
      "expires": 1490867032,
      "identityId": "eu-west-1:bfb4c749-4c0e-446a-892e-defd9b01bed0",
      "secretKey": "XXXX",
      "sessionId": "abc",
      "accountId": 2
   }
```

* `/create/[accountId]/[sessionId]`: Creates a new session, or refreshes the given account and session (invalidating any existing keys).
* `/create/[accountId]/[sessionId]/[userId]`: Creates a new session as above, but also associates a userID with it, to facilitate dispatching events on a per-user basis.
* `/destroy/[accountId]/[sessionId]`: Destroys a session and it's SQS queue.  Note that simply abandoning a session will result in removal of the queue and temporary user automatically, so this is nice but not required.
* `/renew/[accountId]/[sessionId]`: Obtain a new access key for a given session.
* `/status/[accountId]/[sessionId]`: Return the status (sqs queue URL and access keys) of a given session, if it is still active.
* `/status`: Return list of all active sessions.
* `/cleanup`: Identity unused queues and remove them

Typically you will `create` a session when a user logs in, generating a session ID, and `renew` it periodically as the access key approaches expiration.


## Dispatching Messages

Messages can be dispatched via Kinesis by posting JSON objects to the stream.  The entire JSON object is passed, and three fields are used for routing to the SQS queues by filtering against values set during session creation:

* `accountId`: required, must be an integer
* `sessionId`: optional, must be a string
* `userId`: optional, must be an integer

Messages can also be submitted via an API Gateway.  

* `/notify/[accountId]`
* `/notify/[accountId]/session/[sessionId]`
* `/notify/[accountId]/user/[userId]`

Each of these accepts query string parameters, and are used to construct the message.  For example:

`/notify/12345?msg=Hello+World`

Will send the folling JSON to every queue associated with the given account ID:

```
   {
      "accountId": 12345,
      "msg": "Hello World"
   }
```


## Deploying

Deployment requires installation of serverless (http://serverless.com), and this project was  built with version 1.9.  Deployments require an environment name and a region.  For example:

```
# serverless deploy --env prod --region eu-west-1
```

You can also specify a Cognito pool name, and a version tag:

```
# serverless deploy --env prod --region eu-west-1 --poolname my-cognito-pool --version 1.0.5
```

