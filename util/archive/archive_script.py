# archive_script.py
#
# Archive free user data
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import time
import os
import sys
import json
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation, NoSectionError, NoOptionError

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("archive_script_config.ini")

"""A14
Archive free user results files
"""

def handle_archive_queue(sqsQueue=None, s3=None, glacierVault=None, dynamodbTable=None, maxMessages=None, waitTime = None):

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
            archieveRequestData = json.loads(messageBody)
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

        # Process messages --> archive results file
        try:
            jobId = archieveRequestData.get("job_id")
            bucket = archieveRequestData.get("bucket")
            resultFileKey = archieveRequestData.get("result_file_key")
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

        if not jobId or not bucket or not resultFileKey:
            print("Error: Missing job ID, S3 bucket, or S3 key")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        # check if the user is free user
        try:
            response = dynamodbTable.query(KeyConditionExpression="job_id = :jid",
                                   ExpressionAttributeValues={":jid": jobId},
                                   ProjectionExpression="user_id, job_status, completion_time")
        except ClientError as e:
            print(f"Failed to get annotation info from DanamoDB: {e}")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        if len(response["Items"]) == 0:
            print(f"Failed to get job info from DynamoDB")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        if len(response["Items"]) > 1:
            print(f"Failed to get job info from DynamoDB")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        item = response["Items"][0]
        try:
            job_user_id = item["user_id"]
            status = item["job_status"]
            completion_time = item["completion_time"]
        except KeyError as e:
            print(f"Miss value in the dynamodb annotation record: {e}")
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

        try:
            userProfile = helpers.get_user_profile(id=job_user_id)
            userRole = userProfile[4]
        except ClientError as e:
            print(f"Failed to get email recipient: {e}")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        print(userRole)
        if userRole != "free_user":
            print("The user is not free user, no need to archive the annotation result")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        # check if the time is greater or equal to 5 minutes
        if status != "COMPLETED" or int(time.time())-completion_time < config.getint("glacier", "FreeUserArchiveWaitTime"):
            print(f"The job has not completed yet, or has not reached the archive time, cannot archive the result")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        # get result file from s3
        # https://stackoverflow.com/questions/41833565/s3-buckets-to-glacier-on-demand-is-it-possible-from-boto3-api
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
        try:
            resultObject = s3.get_object(Bucket=bucket, Key=resultFileKey)
            resultBody = resultObject["Body"].read()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"The result file does not exist in the s3 bucket: {e}")
            else:
                print(f"ClientError: {e}")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        # Put result file into Glacier
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/vault/index.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/vault/upload_archive.html
        try:
            archiveResponse = glacierVault.upload_archive(body=resultBody)
            glacierObjectId = archiveResponse.id
        except ClientError as e:
            print(f"Failed to archive result file into glacier: {e}")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue
        print(f"Put Result file of job {jobId} to Glacier successfully")

        # Delete result file in s3 bucket after archiving in Glacier
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
        try:
            s3.delete_object(Bucket=bucket, Key=resultFileKey)
        except ClientError as e:
            print(f"Failed to delete result file in s3 bucket after archiving: {e}")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue
        print(f"Delete result file of job {jobId} from s3 successfully")

        # Update results file archive id in DynamoDB
        try:
            dynamodbTable.update_item(
                Key={"job_id": jobId},
                UpdateExpression="SET results_file_archive_id = :archive_id",
                ConditionExpression="job_status = :status",
                ExpressionAttributeValues={
                    ":archive_id": glacierObjectId,
                    ":status": "COMPLETED"
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

        # Delete message from archive request queue
        try:
            message.delete()
        except ClientError as e:
            print(f"Delete archive request message failed: {e}")
        except Exception as e:
            print(str(e))


def main():

    # Get handles to resources
    try:
        region_name = config.get("aws", "AwsRegionName")
    except NoSectionError as e:
        print(f"Can't find section 'aws' from the archive script configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'AwsRegionName' from the archive script configuration file: {e}")
        raise

    sqs = boto3.resource("sqs", region_name=region_name)

    try:
        queue_name = config.get("sqs", "ArchivesQueueName")
    except NoSectionError as e:
        print(f"Can't find section 'sqs' from the archive script configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'queueName' from the archive script configuration file: {e}")
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
        print(f"Can't find section 'sqs' from the archive script configuration file: {e}")
    except NoOptionError as e:
        wait_time = None
        max_messages = None
        print(f"Can't find the option from the archive script configuration file: {e}")

    s3 = boto3.client("s3")

    dynamodb = boto3.resource("dynamodb", region_name=region_name)

    try:
        table_name = config.get("gas", "AnnotationsTable")
    except NoSectionError as e:
        print(f"Can't find section 'gas' from the archive script configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'AnnotationsTable' from the archive script configuration file: {e}")
        raise

    try:
        table = dynamodb.Table(table_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"DynamoDB table not found: {e}")
        else:
            print(f"ClientError: {e}")
        raise

    # glacier resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/vault/index.html
    glacier = boto3.resource("glacier", region_name=region_name)
    try:
        account_id = config.get("glacier", "AccountId")
        vault_name = config.get("glacier", "VaultName")
    except NoSectionError as e:
        print(f"Can't find section '' from the archive script configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option from the archive script configuration file: {e}")
        raise
    try:
        vault = glacier.Vault(account_id, vault_name)
    except ClientError as e:
        print(f"Failed to get glacier vault: {e}")
        raise

    # Poll queue for new results and process them
    while True:
        handle_archive_queue(sqsQueue=queue, s3=s3, glacierVault=vault, dynamodbTable=table, maxMessages=max_messages, waitTime=wait_time)


if __name__ == "__main__":
    main()

### EOF
