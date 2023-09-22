# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##

import boto3
import json
from botocore.exceptions import ClientError

# Define constants here; no config file is used for Lambdas
REGION_NAME = "us-east-1"
DYNAMODB_TABLE = "annotations"
GLACIER_VAULT = "xxxxx"
RESTORE_QUEUE_URL = "xxxxx"


# Using Lambda with Amazon SQS
# https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # parse message
        # https://docs.aws.amazon.com/amazonglacier/latest/dev/api-describe-job-get.html
        # https://beabetterdev.com/2022/12/04/aws-sqs-lambda-setup-tutorial/
        try:
            sqsMessage = json.loads(record['body'])
            sqsMessageBody = json.loads(sqsMessage['Message'])
            archiveJobId = sqsMessageBody['JobId']
            archiveId = sqsMessageBody['ArchiveId']
            description = sqsMessageBody['JobDescription']
            StatusCode = sqsMessageBody['StatusCode']
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Failed to parse record: {e}")
            continue

        if StatusCode != 'Succeeded':
            print("Thaw job not completed")
            continue

        if description:
            try:
                descriptionJson = json.loads(description)
                gasJobId = descriptionJson['gasJobId']
                s3ResultBucket = descriptionJson['s3ResultBucket']
                s3ResultFileKey = descriptionJson['s3ResultFileKey']
            except (KeyError, json.JSONDecodeError) as e:
                print(f"Failed to parse description: {e}")

        else:
            print("Miss description")

        # get the temporary location of the restored object
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/describe_job.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
        glacier = boto3.client('glacier', region_name=REGION_NAME)
        try:
            response = glacier.get_job_output(vaultName=GLACIER_VAULT, jobId=archiveJobId)
        except ClientError as e:
            print(f"Failed to get glacier restore job information: {e}")

        print(response)

        try:
            objectData = response['body'].read()
        except KeyError as e:
            print(f"Failed to get temporary location of the restored object: {e}")

        # copy the thawed object to the gas-results bucket
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        s3 = boto3.client("s3")
        try:
            s3.put_object(Body=objectData, Bucket=s3ResultBucket, Key=s3ResultFileKey)
            print(f"copy the thawed object to the gas-results bucket successfully. gas job id: {gasJobId}")
        except ClientError as e:
            print(f"Failed to copy the thawed object to the gas-results bucket: {e}")

        # delete the archived object from Glacier
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
        try:
            glacier.delete_archive(vaultName=GLACIER_VAULT, archiveId=archiveId)
            print(f"delete the archived object from Glacier successfully. gas job id: {gasJobId}")
        except ClientError as e:
            print(f"Failed to delete the archived object from Glacier: {e}")

        # delete results file archive id in DynamoDB
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
        dynamodb = boto3.resource("dynamodb", region_name=REGION_NAME)
        try:
            dynamodbTable = dynamodb.Table(DYNAMODB_TABLE)
            dynamodbTable.update_item(
                Key={"job_id": gasJobId},
                UpdateExpression="REMOVE results_file_archive_id",
                ConditionExpression="attribute_exists(results_file_archive_id)")
            print(f"delete results file archive id in DynamoDB successfully. gas job id: {gasJobId}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                print(f"Key doesn't exit failed to update job status: {e}")
            elif e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                print(f"Condition failed, failed to update job status: {e}")
            else:
                print(f"ClientError: {e}")
        except Exception as e:
            print(str(e))

        # lambda will delete the message form sqs automatically
        # https://stackoverflow.com/questions/57460190/sqs-deleting-automatically-messages-after-receiving-them-by-lambda