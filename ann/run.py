# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##

import json
import os
import sys
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

import driver

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)
from util import helpers

"""A rudimentary timer for coarse-grained profiling
"""

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation, NoSectionError, NoOptionError

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


if __name__ == "__main__":
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], "vcf")

        # Get results file and log file
        inputFileLocalPath = sys.argv[
            1]  # eg. /home/ubuntu/gas/ann/job/<user_id>/87df1997-8859-47fe-96d3-0e54f8aad6ea/free_1.vcf
        fileNameFull = os.path.splitext(inputFileLocalPath)[
            0]  # eg. /home/ubuntu/gas/ann/job/<user_id>/87df1997-8859-47fe-96d3-0e54f8aad6ea/free_1
        singleJobFolder = os.path.dirname(
            inputFileLocalPath)  # eg. /home/ubuntu/gas/ann/job/<user_id>/87df1997-8859-47fe-96d3-0e54f8aad6ea
        userJobFolder = os.path.dirname(os.path.abspath(singleJobFolder))
        jobId = fileNameFull.split("/")[-2]  # eg. 87df1997-8859-47fe-96d3-0e54f8aad6ea
        userId = fileNameFull.split("/")[-3]
        fileName = os.path.splitext(os.path.basename(inputFileLocalPath))[0]  # eg. free_1
        resultFileLocalPath = fileNameFull + ".annot.vcf"
        logFileLocalPath = fileNameFull + ".vcf.count.log"
        if not os.path.exists(inputFileLocalPath):
            raise FileNotFoundError("Input file not found")
        if not os.path.exists(resultFileLocalPath):
            raise FileNotFoundError("Result file not found at: " + resultFileLocalPath)
        if not os.path.exists(logFileLocalPath):
            raise FileNotFoundError("Log file not found at: " + logFileLocalPath)

        # Upload the results file and log file to S3 results bucket
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
        # https://stackoverflow.com/questions/59258047/boto3-handle-clienterror-during-s3-file-upload
        s3 = boto3.client("s3")

        try:
            bucket = config.get("s3", "ResultsBucketName")
            keyPrefix = config.get("s3", "KeyPrefix")
        except NoSectionError as e:
            print(f"Can't find section 'aws' from the annotator configuration file: {e}")
            raise
        except NoOptionError as e:
            print(f"Can't find option from the annotator configuration file: {e}")
            raise

        resultFilekey = keyPrefix + userId + "/" + jobId + "~" + fileName + ".annot.vcf"
        logFilekey = keyPrefix + userId + "/" + jobId + "~" + fileName + ".vcf.count.log"
        try:
            s3.upload_file(resultFileLocalPath, bucket, resultFilekey)
            print("Upload result file to S3 results bucket successfully")
        except ClientError as e:
            print(f"Failed to upload result file {resultFileLocalPath} to S3:", e)
        try:
            s3.upload_file(logFileLocalPath, bucket, logFilekey)
            print("Upload log file to S3 results bucket successfully")
        except ClientError as e:
            print(f"Failed to upload log file {logFileLocalPath} to S3:", e)

        # Update DynamoDB
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
        # https://gist.github.com/pictolearn/99ae4e93f0f7995c2b8e034d17df67d9
        try:
            regionName = config.get("aws", "AwsRegionName")
        except NoSectionError as e:
            print(f"Can't find section 'aws' from the annotator configuration file: {e}")
            raise
        except NoOptionError as e:
            print(f"Can't find option 'AwsRegionName' from the annotator configuration file: {e}")
            raise

        dynamodb = boto3.resource("dynamodb", region_name=regionName)

        try:
            tableName = config.get("gas", "AnnotationsTable")
        except NoSectionError as e:
            print(f"Can't find section 'gas' from the annotator configuration file: {e}")
            raise
        except NoOptionError as e:
            print(f"Can't find option 'AnnotationsTable' from the annotator configuration file: {e}")
            raise

        try:
            table = dynamodb.Table(tableName)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                print("Update DynamoDB failed, DynamoDB table not found" + str(e))
            else:
                print("Update DynamoDB failed, ClientError" + str(e))
        completeEpochTime = int(time.time())
        try:
            table.update_item(
                Key={"job_id": jobId},
                UpdateExpression="SET s3_results_bucket = :resultsBucket, s3_key_result_file = :resultsKey, "
                                 "s3_key_log_file = :logKey, completion_time = :completionTime, job_status = :newStatus",
                ExpressionAttributeValues={
                    ":resultsBucket": bucket,
                    ":resultsKey": resultFilekey,
                    ":logKey": logFilekey,
                    ":completionTime": completeEpochTime,
                    ":newStatus": "COMPLETED"
                })
            print("Update job in DynamoDB successfully.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                print("Update DynamoDB failed, Key doesn't exit" + str(e))
            elif e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                print("Update DynamoDB failed, Condition failed" + str(e))
            else:
                print("Update DynamoDB failed, ClientError" + str(e))
        except Exception as e:
            print("Update DynamoDB failed" + str(e))

        # Convert completeEpochTime to a human-readable form
        # https://stackoverflow.com/questions/12978391/localizing-epoch-time-with-pytz-in-python
        try:
            completeDt = datetime.fromtimestamp(int(completeEpochTime))  # convert timestamp to datetime
            completeTime = completeDt.strftime("%Y-%m-%d %H:%M:%S")  # format as string
        except ValueError as e:
            # if the time conversion failed, just display the epoch time, so that it won't ruin the whole file
            completeTime = completeEpochTime
            print(f"Error happened wen cast submit epoch time to CST date time: {e}")

        # publishes a notification to the results topic when the job is complete
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/topic/publish.html
        # https://boto3.amazonaws.com/v1/documentation/api/1.14.33/reference/services/sns.html
        # https://docs.aws.amazon.com/sns/latest/api/API_Publish.html
        sns = boto3.resource("sns", region_name=regionName)
        try:
            resultsTopic = config.get("sns", "ResultsTopicArn")
            GasHomePageUrl = config.get("sns", "GasHomePageUrl")
        except NoSectionError as e:
            print(f"Can't find section 'sns' from the annotator configuration file: {e}")
            raise
        except NoOptionError as e:
            print(f"Can't find options from the annotator configuration file: {e}")
            raise

        topic = sns.Topic(resultsTopic)
        subject = f"Results available for job {jobId}"
        body = f"Your annotation job completed at {completeTime}. Click here to view job details and results: {GasHomePageUrl}/annotations/{jobId}."
        message = {"userId": userId, "subject": subject, "body": body}
        try:
            topic.publish(Message=json.dumps(message))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NotFound":
                print(f"SNS topic not found: {e}")
            elif e.response["Error"]["Code"] == "InvalidParameter":
                print(f"Invalid parameter for publish: {e}")
            else:
                print(f"ClientError: {e}")
        except Exception as e:
            print(str(e))

        # Clean up (delete) local job files
        # https://www.w3schools.com/python/python_file_remove.asp
        try:
            os.remove(inputFileLocalPath)
        except OSError as e:
            print(f"Fail to delete input file at {inputFileLocalPath}: {e}")
        try:
            os.remove(resultFileLocalPath)
        except OSError as e:
            print(f"Fail to delete result file at {resultFileLocalPath}: {e}")
        try:
            os.remove(logFileLocalPath)
        except OSError as e:
            print(f"Fail to delete log file at {logFileLocalPath}: {e}")

        # Delete the job folder, which is supposed to be empty at this point
        # https://www.geeksforgeeks.org/delete-a-directory-or-file-using-python/
        try:
            os.rmdir(singleJobFolder)
            print("Clean up local job files successfully")
        except OSError as e:
            print(f"Fail to delete the job folder at {singleJobFolder}: {e}")

        # Delete the user folder if no job remains in it
        if not os.listdir(userJobFolder) :
            try:
                os.rmdir(userJobFolder)
            except OSError as e:
                print(f"Fail to delete the user job folder at {userJobFolder}: {e}")

        # if the user is free user, call a step function which will wait 5 minutes and call a lambda function
        # The lambda function will check the user's role again and call sns send archive message to sqs
        # Reference for step function
        # https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-wait-state.html
        # https://docs.aws.amazon.com/code-library/latest/ug/python_3_sfn_code_examples.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/start_execution.html
        # https://docs.aws.amazon.com/step-functions/latest/dg/task-timer-sample.html
        # https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html
        # Reference for Lambda package install and event handler:
        # https://docs.aws.amazon.com/zh_cn/lambda/latest/dg/python-package.html
        # https://stackoverflow.com/questions/44855531/no-module-named-psycopg2-psycopg-modulenotfounderror-in-aws-lambda
        # https://github.com/a-j/awslambda-psycopg2
        # https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html
        # Get user's role using function in helpers.py
        try:
            userProfile = helpers.get_user_profile(id=userId)
            userRole = userProfile[4]
        except ClientError as e:
            print(f"Failed to get userRole: {e}")
        if userRole == "free_user":
            lambdaInputParams = {
                "userId": userId,
                "jobId": jobId,
                "bucket": bucket,
                "resultFilekey": resultFilekey
            }
            try:
                stateMachineArn = config.get("sfn", "StateMachineArn")
            except NoSectionError as e:
                print(f"Can't find section 'aws' from the annotator configuration file: {e}")
            except NoOptionError as e:
                print(f"Can't find option 'AwsRegionName' from the annotator configuration file: {e}")

            stepFunction = boto3.client("stepfunctions", region_name=regionName)
            try:
                stepFunction.start_execution(stateMachineArn=stateMachineArn,
                                             input=json.dumps(lambdaInputParams))
            except ClientError as e:
                print("Failed to call step function" + str(e))

    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF
