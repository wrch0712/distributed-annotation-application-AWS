# thaw_script.py
#
# Thaws upgraded (premium) user data
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
config.read("thaw_script_config.ini")

"""A16
Initiate thawing of archived objects from Glacier
"""

def handle_thaw_queue(thawsQueue=None, glacier=None, vaultName=None, restoresTopic=None, maxMessages=None, waitTime=None):

    # Read messages from the queue
    messages = []
    try:
        messages = thawsQueue.receive_messages(MaxNumberOfMessages=maxMessages, WaitTimeSeconds=waitTime)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            print(f"Access denied to the queue: {e}")
        else:
            print(f"ClientError: {e}")
    except Exception as e:
        print(str(e))

    # Process messages --> initiate restore from Glacier
    for message in messages:
        # Get data from message body
        try:
            messageBody = json.loads(message.body).get("Message")
            thawRequestData = json.loads(messageBody)
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

        try:
            gasJobId = thawRequestData.get("job_id")
            s3ResultBucket = thawRequestData.get("s3_results_bucket")
            s3ResultFileKey = thawRequestData.get("result_file_s3_key")
            archiveId = thawRequestData.get("results_file_archive_id")
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

        if not gasJobId or not s3ResultFileKey or not s3ResultBucket or not archiveId:
            print("Error: Missing results file job id or archive id")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        # initiate restore from Glacier
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/restoring-objects-retrieval-options.html
        # https://docs.aws.amazon.com/zh_cn/amazonglacier/latest/dev/configuring-notifications.html
        description = {
            "gasJobId": gasJobId,
            "s3ResultBucket": s3ResultBucket,
            "s3ResultFileKey": s3ResultFileKey
        }
        try:
            # Attempt Expedited retrieval
            response = glacier.initiate_job(
                vaultName=vaultName,
                jobParameters={
                    'Type': 'archive-retrieval',
                    'ArchiveId': archiveId,
                    'Description': json.dumps(description),
                    'SNSTopic': restoresTopic,
                    'Tier': 'Expedited'
                }
            )
            restoreJobId = response['jobId']
            print(f"Expedited retrieval request submitted. restore job id: {restoreJobId}, gas job id: {gasJobId}")
        except glacier.exceptions.InsufficientCapacityException:
            # Expedited retrieval failed, retry with Standard retrieval
            try:
                response = glacier.initiate_job(
                    vaultName=vaultName,
                    jobParameters={
                        'Type': 'archive-retrieval',
                        'ArchiveId': archiveId,
                        'Description': json.dumps(description),
                        'SNSTopic': restoresTopic,
                        'Tier': 'Standard'
                    }
                )
                restoreJobId = response['jobId']
                print(f"Expedited retrieval failed, standard retrieval request submitted. restore job id: {restoreJobId}, gas job id: {gasJobId}")
            except ClientError as e:
                print(f"Failed to initiate standard restore from Glacier: {e}")
        except ClientError as e:
            print(f"Failed to initiate restore from Glacier: {e}")

        # Delete messages
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
        queue_name = config.get("sqs", "ThawsQueueName")
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

    glacier = boto3.client('glacier', region_name=region_name)
    try:
        vault_name = config.get("glacier", "VaultName")
    except NoSectionError as e:
        print(f"Can't find section 'glacier' from the archive script configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'VaultName' from the archive script configuration file: {e}")
        raise

    try:
        restores_topic = config.get("sns", "RestoresTopicArn")
    except NoSectionError as e:
        print(f"Can't find section 'sns' from the archive script configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'RestoresTopicArn' from the archive script configuration file: {e}")
        raise

    # Poll queue for new results and process them
    while True:
        handle_thaw_queue(thawsQueue=queue, glacier=glacier, vaultName=vault_name, restoresTopic=restores_topic, maxMessages=max_messages, waitTime=wait_time)


if __name__ == "__main__":
    main()

### EOF
