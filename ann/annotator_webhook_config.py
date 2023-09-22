# ann_config.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Set GAS annotator configuration options
#
##

import os

base_dir = os.path.abspath(os.path.dirname(__file__))

# Get the IAM username that was stashed at launch time
try:
    with open("/home/ubuntu/.launch_user", "r") as file:
        iam_username = file.read().replace("\n", "")
except FileNotFoundError as e:
    if "LAUNCH_USER" in os.environ:
        iam_username = os.environ["LAUNCH_USER"]
    else:
        # Unable to set username, so exit
        print("Unable to find launch user name in local file or environment!")
        raise e


class Config(object):

    CSRF_ENABLED = True

    ANNOTATOR_BASE_DIR = f"{base_dir}"
    ANNOTATOR_JOBS_DIR = f"{base_dir}/jobs"

    AWS_REGION_NAME = "us-east-1"

    # AWS S3 upload parameters
    AWS_S3_INPUTS_BUCKET = "gas-inputs"
    AWS_S3_RESULTS_BUCKET = "gas-results"

    # AWS SNS topics
    AWS_SNS_JOB_REQUEST_TOPIC = (
        "xxxxx"
    )

    # AWS SQS queues
    AWS_SQS_WAIT_TIME = 20
    AWS_SQS_MAX_MESSAGES = 10
    AWS_SQS_REQUESTS_QUEUE_NAME = "xxxxx"

    # AWS DynamoDB
    AWS_DYNAMODB_ANNOTATIONS_TABLE = "xxxxx"

### EOF
