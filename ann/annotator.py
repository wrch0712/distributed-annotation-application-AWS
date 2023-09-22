# annotator.py
#
# NOTE: This file lives on the AnnTools instance
#
# Copyright (C) 2013-2023 Vas Vasiliadis
# University of Chicago
##

import subprocess
import boto3
import time
import os
import sys
import json
from botocore.exceptions import ClientError

base_dir = os.path.abspath(os.path.dirname(__file__))

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation, NoSectionError, NoOptionError

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")

"""Reads request messages from SQS and runs AnnTools as a subprocess.

Move existing annotator code here
"""

# Create job folder if it doesn't exist
jobFolder = os.path.join(base_dir, "job")
if not os.path.exists(jobFolder):
    os.makedirs(jobFolder)


def handle_requests_queue(sqsQueue=None, s3=None, dynamodbTable=None, maxMessages=None, waitTime = None):

    # configparser： https://docs.python.org/3/library/configparser.html
    # configparser error handling: https://stackoverflow.com/questions/24832628/python-configparser-getting-and-setting-without-exceptions
    # Read messages from the queue
    messages = []
    try:
        messages = sqsQueue.receive_messages(MaxNumberOfMessages=maxMessages, WaitTimeSeconds=waitTime)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            print(f"Access denied to the queue: {e}")
        else:
            print(f"ClientError: {e}")
    except Exception as e:
        print(str(e))

    # Process messages
    for message in messages:
        # Get data from message body
        try:
            messageBody = json.loads(message.body).get("Message")
            data = json.loads(messageBody)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON: {e}")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue
        except Exception as e:
            print(str(e))
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        # Extract parameters
        try:
            jobId = data.get("job_id")
            userId = data.get("user_id")
            bucket = data.get("s3_inputs_bucket")
            key = data.get("s3_key_input_file")
        except AttributeError as e:
            print(f"AttributeError: {e}")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue
        except Exception as e:
            print(str(e))
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        if not jobId or not userId or not bucket or not key:
            print("Error: Missing job ID, user ID, S3 bucket, or S3 key")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        # Downloads the input file from S3 and saves it to the AnnTools instance in /home/ubuntu/gas/ann/job/<user_id>/<job_id> folder
        singleJobFolder = os.path.join(jobFolder, userId, jobId)
        try:
            os.makedirs(singleJobFolder)
        except FileExistsError as e:
            print(f"Job folder has already in annotator: {e}")
            continue

        filename = key.split("~")[-1]
        localPath = os.path.join(singleJobFolder, filename)

        try:
            s3.download_file(bucket, key, localPath)
        except ClientError as e:
            print(f"Cannot download the input file from s3: {e}")
            continue

        if not os.path.exists(localPath):  # if file is not found
            print("Cannot find the file in the AnnTools instance")
            continue

        # Launch annotation job as a background process
        try:
            subprocess.Popen(["python", os.path.join(base_dir, "run.py"), localPath])
        except subprocess.CalledProcessError as e:
            print(f"Subprocess failed, failed to launch annotator job: {e}")
            continue
        except Exception as e:
            print(str(e))
            continue

        # Delete message from queue, if job was successfully submitted
        try:
            message.delete()
        except ClientError as e:
            print(f"Delete message failed: {e}")
        except Exception as e:
            print(str(e))

        # Update job_status in DynamoDB to “RUNNING” if its current status is “PENDING”
        try:
            dynamodbTable.update_item(
                Key={"job_id": jobId},
                UpdateExpression="SET job_status = :newStatus",
                ConditionExpression="job_status = :oldStatus",
                ExpressionAttributeValues={
                    ":newStatus": "RUNNING",
                    ":oldStatus": "PENDING"
                })
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                print(f"Key doesn't exit failed to update job status: {e}")
            elif e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                print(f"Condition failed, failed to update job status: {e}")
            else:
                print(f"ClientError: {e}")
        except Exception as e:
            print(str(e))


def main():
    # Get handles to resources
    # configparser： https://docs.python.org/3/library/configparser.html
    # configparser error handling: https://stackoverflow.com/questions/24832628/python-configparser-getting-and-setting-without-exceptions
    try:
        region_name = config.get("aws", "AwsRegionName")
    except NoSectionError as e:
        print(f"Can't find section 'aws' from the annotator configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'AwsRegionName' from the annotator configuration file: {e}")
        raise

    sqs = boto3.resource("sqs", region_name=region_name)

    try:
        queue_name = config.get("sqs", "QueueName")
    except NoSectionError as e:
        print(f"Can't find section 'sqs' from the annotator configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'queueName' from the annotator configuration file: {e}")
        raise

    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "QueueDoesNotExist":
            print(f"Can't find the queue, queue name is wrong: {e}")
        else:
            print(f"ClientError: {e}")
        raise
    except Exception as e:
        print(str(e))
        raise

    try:
        wait_time = config.getint("sqs", "WaitTime")
        max_messages = config.getint("sqs", "MaxMessages")
    except ValueError as e:
        wait_time = None
        max_messages = None
        print(f"ValueError: {e}")
    except NoSectionError as e:
        wait_time = None
        max_messages = None
        print(f"Can't find section 'sqs' from the annotator configuration file: {e}")
    except NoOptionError as e:
        wait_time = None
        max_messages = None
        print(f"Can't find the option from the annotator configuration file: {e}")

    s3 = boto3.client("s3")
    dynamodb = boto3.resource("dynamodb", region_name=region_name)

    try:
        table_name = config.get("gas", "AnnotationsTable")
    except NoSectionError as e:
        print(f"Can't find section 'gas' from the annotator configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'AnnotationsTable' from the annotator configuration file: {e}")
        raise

    try:
        table = dynamodb.Table(table_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"DynamoDB table not found: {e}")
        else:
            print(f"ClientError: {e}")
        raise

    # Poll queue for new results and process them
    while True:
        handle_requests_queue(sqsQueue=queue, s3=s3, dynamodbTable=table, maxMessages=max_messages, waitTime=wait_time)


if __name__ == "__main__":
    main()
### EOF
