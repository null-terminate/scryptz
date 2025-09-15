#!/usr/bin/env python
import boto3
import json
import asyncio
import datetime
import uuid
import time

class RpcRequest:
    def __init__(self, path, body, headers={}):
        self.path = path
        self.method = 'POST'
        self.body = body
        self.headers = headers

    def toJson(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

client = boto3.client('lambda')

def invoke_lambda(fn, r):
    res = client.invoke(FunctionName=fn, Payload=r.toJson())
    return res 

async def main():
    print('<main>')
    
    # main logic!
    s3_bucket = 'blah'
    s3_obj_prefix = 'file/prefix'
    sqs_queue_url = 'https://sqs.eu-west-1.amazonaws.com/{account}/my-queue'
    
    # Initialize S3 and SQS clients
    s3_client = boto3.client('s3')
    sqs_client = boto3.client('sqs')
    
    # Initialize message batch
    message_batch = []
    
    # List all objects in the S3 bucket with the given prefix recursively
    print(f"Listing objects in bucket '{s3_bucket}' with prefix '{s3_obj_prefix}'")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': s3_bucket, 'Prefix': s3_obj_prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    
    object_count = 0
    
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                object_key = obj['Key']

                # skip "folder" placeholders
                if object_key.endswith("/"):
                    continue

                print(f"Found object: {object_key}")
                object_count += 1

                # Create S3 event payload
                current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                s3_event = {
                    "version": "0",
                    "id": str(uuid.uuid4()).replace("-", ""),
                    "detail-type": "Object Created",
                    "source": "aws.s3",
                    "account": "ACCOUNT_ID",  # Replace with your account ID if needed
                    "time": current_time,
                    "region": "REGION",  # Replace with your region
                    "resources": [
                        f"arn:aws:s3:::{s3_bucket}"
                    ],
                    "detail": {
                        "version": "0",
                        "bucket": {
                            "name": s3_bucket
                        },
                        "object": {
                            "key": object_key,
                            "size": obj.get('Size', 0),
                            "etag": obj.get('ETag', '').strip('"'),
                            "version-id": obj.get('VersionId', ''),
                            "sequencer": f"{int(time.time()*1000):013d}"
                        },
                        "request-id": str(uuid.uuid4()).replace("-", ""),
                        "requester": "ACCOUNT_ID",  # Replace with your account ID if needed
                        "reason": "PutObject"
                    }
                }                

                # Add message to batch
                payload = json.dumps(s3_event)
                
                # Add to message batch (SQS batch limit is 10)
                message_batch.append({
                    'Id': str(len(message_batch)),
                    'MessageBody': payload
                })
                
                # Send batch when it reaches 10 messages
                if len(message_batch) >= 10:
                    try:
                        response = sqs_client.send_message_batch(
                            QueueUrl=sqs_queue_url,
                            Entries=message_batch
                        )
                        print(f"Batch of {len(message_batch)} messages sent to SQS")
                        if 'Failed' in response and response['Failed']:
                            print(f"Failed to send {len(response['Failed'])} messages: {response['Failed']}")
                        message_batch = []
                    except Exception as e:
                        print(f"Error sending batch to SQS: {str(e)}")
    
    # Send any remaining messages in the batch
    if message_batch:
        try:
            response = sqs_client.send_message_batch(
                QueueUrl=sqs_queue_url,
                Entries=message_batch
            )
            print(f"Final batch of {len(message_batch)} messages sent to SQS")
            if 'Failed' in response and response['Failed']:
                print(f"Failed to send {len(response['Failed'])} messages: {response['Failed']}")
        except Exception as e:
            print(f"Error sending final batch to SQS: {str(e)}")
    
    print(f"Total objects processed: {object_count}")
    print('</main>')
   
if __name__ == '__main__':
    print('start!')
    start_time = datetime.datetime.now()

    asyncio.run(main())

    duration = datetime.datetime.now() - start_time
    print('done: ' + str(duration))
