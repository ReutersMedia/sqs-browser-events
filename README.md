
## Overview

This package provides a mechanism for serverless long-polling for browsers, using AWS SQS and Lambda functions, and Cognito for creating temporary unauthorized sessions.  Typical use would be a backend system that creates a new session upon login, and passes an SQS Queue URL and a pair of AWS auth keys to the browser, and the browser then polls SQS for events.  A dispatcher API and Kinesis input stream is used to generate and dispatch events to the queues.


## Creating a Cognito Identity Pool

A stack needs a Cognito user pool, and this cannot be set up via Cloud Formation.  One identity pool however can be shared by a number of stacks, and the serverless.yml file requires a pool with the name "sqs_browser".  

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

## Session Manager API

In the API calls below, the accountId must be an integer, and sessionId must be a string with length less than 256 characters.  For the create, renew, and status endpoints, the response will contain session descriptions of the form given below.  The expiration time is for the access key and will be several hours in the future.  These access keys can be renewed periodically by calling the `renew` endpoint.

```
   {
      "sqsUrl": "https://eu-west-1.queue.amazonaws.com/XXXXXXXXX/kentest1-brws-KLK0dJ6AN9USg6CAx8FXVk52G2M",
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
* `/notify/[accountId]/session/[sessionId]/user/[userId]`
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
