import os
import boto3
import json
import time
import urllib
import pandas as pd


sqs = boto3.client('sqs', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
lambda_client = boto3.client('lambda', region_name='us-east-1')
request_queue_url = "https://sqs.us-east-1.amazonaws.com/125035222225/ec3_openstack_request_queue"
response_queue_url = "https://sqs.us-east-1.amazonaws.com/125035222225/ec3_openstack_response_queue"
lambda_arn = "arn:aws:lambda:us-east-1:125035222225:function:my_facerecognition_ec3"
# temp_path = "/Users/bhrugudave/Downloads/temporary/"
temp_path = "/tmp/"
output_file = "output.txt"


def poll_from_sqs(queue_url):
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=10,
        MessageAttributeNames=[
            'All'
        ],
        WaitTimeSeconds=1
    )
    if 'Messages' in response:
        return response['Messages']
    return []


def get_messages_from_response_queue(queue_url, num_of_incoming_reqs):
    responses = []
    num_of_outgoing_reqs = 0
    while num_of_outgoing_reqs != num_of_incoming_reqs:
        messages = poll_from_sqs(queue_url)
        num_of_outgoing_reqs += len(messages)
        for message in messages:
            receipt_handle = message['ReceiptHandle']
            response_body = json.loads(message['Body'])
            response = {
                "FileName": response_body['name'] + ".JPEG",
                "Prediction": response_body['prediction'],
            }
            responses.append(response)

            sqs.delete_message(
                QueueUrl=response_queue_url,
                ReceiptHandle=receipt_handle
            )
        time.sleep(1)

    return responses


def poll_from_request_queue():
    print("STARTING TO POLL THE REQUEST QUEUE")
    while True:
        messages = poll_from_sqs(request_queue_url)
        num_of_outgoing_message = len(messages)
        for message in messages:
            print("RECEIVED REQUEST MESSAGE: " + str(message))
            receipt_handle = message['ReceiptHandle']
            event = json.loads(message['Body'])
            lambda_response = invoke_lambda_function(event)
            if lambda_response['StatusCode'] == 202:
                print("DELETING MESSAGE FROM REQUEST SQS")
                sqs.delete_message(
                    QueueUrl=request_queue_url,
                    ReceiptHandle=receipt_handle
                )

        poll_from_response_queue()
        time.sleep(1)


def invoke_lambda_function(event):
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType='Event',
        Payload=json.dumps(event),
        LogType='None')

    print("LAMBDA RESPONSE: " + str(response))
    return response


def download_from_s3(file_path, bucket_name, key):
    try:
        s3.download_file(Bucket=bucket_name, Key=key, Filename=file_path)
        print("DOWNLOADED FILE FROM S3")
    except Exception as e:
        print("ERROR WHILE DOWNLOADING FROM S3: " + str(e))


def poll_from_response_queue():
    print("POLLING FROM RESPONSE QUEUE")
    empty_response_counter = 0
    while empty_response_counter < 10:
        messages = poll_from_sqs(response_queue_url)
        if len(messages) == 0:
            print("NOTHING TO POLL FROM RESPONSE QUEUE")
            empty_response_counter += 1
            continue

        for message in messages:
            print("RECEIVED RESPONSE MESSAGE: " + str(message))
            receipt_handle = message['ReceiptHandle']
            event = json.loads(message['Body'])
            bucket = event['Records'][0]['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
            filepath = temp_path + str(key)
            download_from_s3(filepath, bucket, key)
            data = pd.read_csv(filepath)

            with open(temp_path + output_file, 'a') as f:
                print('Filename:', key, file=f)
                print('DATA: ', data.to_json(), file=f)

            f.close()
            print(data)

            print("DELETING MESSAGE FROM RESPONSE SQS")
            sqs.delete_message(
                QueueUrl=response_queue_url,
                ReceiptHandle=receipt_handle
            )


poll_from_request_queue()
