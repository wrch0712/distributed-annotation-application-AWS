# notify.py
#
# Notify users of job completion
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
config.read("notify_config.ini")

"""A12
Reads result messages from SQS and sends notification emails.
"""


def handle_results_queue(emailSender, resultsQueue=None, maxMessages=None, waitTime = None):

    # Read messages from the queue
    messages = []
    try:
        messages = resultsQueue.receive_messages(MaxNumberOfMessages=maxMessages, WaitTimeSeconds=waitTime)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            print(f"Access denied to the queue: {e}")
        else:
            print(f"ClientError: {e}")
    except Exception as e:
        print(str(e))

    # Process messages --> send email to user
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
            userId = data.get("userId")
            emailSubject = data.get("subject")
            emailBody = data.get("body")
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

        if not userId or not emailSubject or not emailBody:
            print("Error: Missing user id, email Subject or email body")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        # Get email recipient
        try:
            userProfile = helpers.get_user_profile(id=userId)
            emailRecipient = userProfile[2]
        except Exception as e:
            print(f"Failed to get email recipient: {e}")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        if "@" not in emailRecipient:
            print(f"Invaild form of email recipient")
            try:
                message.delete()
            except ClientError as e:
                print(f"Delete message failed: {e}")
            continue

        # Send email
        try:
            helpers.send_email_ses(recipients=emailRecipient, sender=emailSender, subject=emailSubject, body=emailBody)
        except ClientError as e:
            print(f"Failed to send notification email: {e}")
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

        # Delete messages from result queue
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/message/delete.html#SQS.Message.delete
        try:
            message.delete()
        except ClientError as e:
            print(f"Delete message failed: {e}")
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
        result_queue_name = config.get("sqs", "ResultsQueueName")
    except NoSectionError as e:
        print(f"Can't find section 'sqs' from the notify configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'ResultsQueueName' from the notify configuration file: {e}")
        raise

    try:
        result_queue = sqs.get_queue_by_name(QueueName=result_queue_name)
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
        print(f"Can't find section 'sqs' from the notify configuration file: {e}")
    except NoOptionError as e:
        wait_time = None
        max_messages = None
        print(f"Can't find the option from the notify configuration file: {e}")

    try:
        email_sender = config.get("gas", "emailSender")
    except NoSectionError as e:
        print(f"Can't find section 'gas' from the notify configuration file: {e}")
        raise
    except NoOptionError as e:
        print(f"Can't find option 'emailSender' from the notify configuration file: {e}")
        raise

    # Poll queue for new results and process them
    while True:
        handle_results_queue(emailSender=email_sender, resultsQueue=result_queue, maxMessages=max_messages, waitTime=wait_time)


if __name__ == "__main__":
    main()

### EOF
