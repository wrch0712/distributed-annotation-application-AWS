# annotator_webhook.py
#
# NOTE: This file lives on the AnnTools instance
# Modified to run as a web server that can be called by SNS to process jobs
# Run using: python annotator_webhook.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##

import json
import os
import subprocess

import boto3
import requests
from botocore.exceptions import ClientError
from flask import Flask, jsonify, request, abort

app = Flask(__name__)
app.url_map.strict_slashes = False

# Get configuration and add to Flask app object
environment = "annotator_webhook_config.Config"
app.config.from_object(environment)

# Connect to SQS and get the message queue
sqs = boto3.resource('sqs', region_name=app.config["AWS_REGION_NAME"])
try:
    requestsQueue = sqs.get_queue_by_name(QueueName=app.config["AWS_SQS_REQUESTS_QUEUE_NAME"])
except ClientError as e:
    if e.response["Error"]["Code"] == "QueueDoesNotExist":
        print(f"Can't find the queue, queue name is wrong: {e}")
    else:
        print(f"ClientError: {e}")
    abort(500)
except Exception as e:
    print(str(e))
    abort(500)

# Connect to DynamoDB and get the annotation table
dynamodb = boto3.resource("dynamodb", region_name=app.config["AWS_REGION_NAME"])
try:
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
except ClientError as e:
    if e.response["Error"]["Code"] == "ResourceNotFoundException":
        print(f"DynamoDB table not found: {e}")
    else:
        print(f"ClientError: {e}")
    abort(500)

# Create job folder if it doesn't exist
if not os.path.exists(app.config["ANNOTATOR_JOBS_DIR"]):
    os.makedirs(app.config["ANNOTATOR_JOBS_DIR"])

@app.route("/", methods=["GET"])
def annotator_webhook():

    return ("Annotator webhook; POST job to /process-job-request"), 200


"""
A13 - Replace polling with webhook in annotator

Receives request from SNS; queries job queue and processes message.
Reads request messages from SQS and runs AnnTools as a subprocess.
Updates the annotations database with the status of the request.
"""


@app.route("/process-job-request", methods=["GET", "POST"])
def annotate():

    print(request)

    if request.method == "GET":

        return jsonify({"code": 405, "error": "Expecting SNS POST request."}), 405

    elif request.method == "POST":

        # Check message type
        # https://docs.aws.amazon.com/sns/latest/dg/sns-message-and-json-formats.html#http-subscription-confirmation-json
        try:
            messageType = request.headers.get("x-amz-sns-message-type")
        except AttributeError as e:
            return (
                jsonify({"code": 500, "message": f"MessageType not found: {e}"}),
                500,
            )
        message = json.loads(request.data)

        # Confirm SNS topic subscription
        # https://docs.aws.amazon.com/sns/latest/dg/sns-message-and-json-formats.html#http-subscription-confirmation-json
        if messageType == "SubscriptionConfirmation":
            sns = boto3.resource("sns", region_name=app.config["AWS_REGION_NAME"])
            try:
                requestsTopic = sns.Topic(app.config["AWS_SNS_JOB_REQUEST_TOPIC"])
            except ClientError as e:
                return (
                    jsonify({"code": 500, "message": f"Can't get the sns request topic: {e}"}),
                    500,
                )

            # confirm subscription
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/topic/confirm_subscription.html
            try:
                response = requestsTopic.confirm_subscription(Token=message.get("Token", ""))
            except ClientError as e:
                return (
                    jsonify({"code": 500, "message": f"Failed to confirm subscription to sns: {e}"}),
                    500,
                )

            return (
                jsonify({"code": 200, "message": f"Confirm subscription successfully; {response}"}),
                200,
            )

        # Process job request
        # https://docs.aws.amazon.com/sns/latest/dg/sns-message-and-json-formats.html#http-notification-json
        if messageType == "Notification":
            # Extract parameters
            messageData = json.loads(message["Message"])
            try:
                jobId = messageData["job_id"]
                userId = messageData["user_id"]
                bucket = messageData["s3_inputs_bucket"]
                key = messageData["s3_key_input_file"]
            except AttributeError as e:
                print(f"AttributeError: {e}")
                return (
                    jsonify({"code": 500, "message": f"AttributeError: {e}"}),
                    500,
                )
            except Exception as e:
                print(str(e))
                return (
                    jsonify({"code": 500, "message": str(e)}),
                    500,
                )

            if not jobId or not userId or not bucket or not key:
                print("Missing job ID, user ID, S3 bucket, or S3 key")
                return (
                    jsonify({"code": 500, "message": "Missing job ID, user ID, S3 bucket, or S3 key"}),
                    500,
                )

            # Downloads the input file from S3 and saves it to the AnnTools instance in /home/ubuntu/gas/ann/job/<user_id>/<job_id> folder
            singleJobFolder = os.path.join(app.config["ANNOTATOR_JOBS_DIR"], userId, jobId)
            if not os.path.exists(singleJobFolder):
                os.makedirs(singleJobFolder)

                filename = key.split("~")[-1]
                localPath = os.path.join(singleJobFolder, filename)
                s3 = boto3.client("s3")
                try:
                    s3.download_file(bucket, key, localPath)
                except ClientError as e:
                    print(f"Cannot download the input file from s3: {e}")
                    return (
                        jsonify({"code": 500, "message": f"Cannot download the input file from s3: {e}"}),
                        500,
                    )

                if not os.path.exists(localPath):  # if file is not found
                    print("Cannot find the file in the AnnTools instance")
                    return (
                        jsonify({"code": 500, "message": "Cannot find the file in the AnnTools instance"}),
                        500,
                    )

                # Launch annotation job as a background process
                try:
                    subprocess.Popen(["python", os.path.join(app.config["ANNOTATOR_BASE_DIR"], "run.py"), localPath])
                except subprocess.CalledProcessError as e:
                    print( f"Subprocess failed, failed to launch annotator job: {e}")
                    return (
                        jsonify({"code": 500, "message": f"Subprocess failed, failed to launch annotator job: {e}"}),
                        500,
                    )
                except Exception as e:
                    print(str(e))
                    return (
                        jsonify({"code": 500, "message": str(e)}),
                        500,
                    )

                # Update job_status in DynamoDB to “RUNNING” if its current status is “PENDING”
                try:
                    table.update_item(
                        Key={"job_id": jobId},
                        UpdateExpression="SET job_status = :newStatus",
                        ConditionExpression="job_status = :oldStatus",
                        ExpressionAttributeValues={
                            ":newStatus": "RUNNING",
                            ":oldStatus": "PENDING"
                        })
                except ClientError as e:
                    print(f"Did not to update job status: {e}")

                # Delete message from queue, if job was successfully submitted
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/message/index.html
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/message/delete.html
                findSqsMessage = False
                while not findSqsMessage:
                    # find the message from the queue
                    try:
                        sqsMessages = requestsQueue.receive_messages(MaxNumberOfMessages=app.config["AWS_SQS_MAX_MESSAGES"], WaitTimeSeconds=app.config["AWS_SQS_WAIT_TIME"])
                    except ClientError as e:
                        if e.response["Error"]["Code"] == "AccessDenied":
                            print(f"Access denied to the queue:: {e}")
                            return (
                                jsonify(
                                    {"code": 500, "message": f"Access denied to the queue:: {e}"}),
                                500,
                            )
                        else:
                            print(f"ClientError: {e}")
                            return (
                                jsonify(
                                    {"code": 500, "message": f"ClientError: {e}"}),
                                500,
                            )
                    except Exception as e:
                        print(str(e))
                        return (
                            jsonify({"code": 500, "message": str(e)}),
                            500,
                        )

                    if len(sqsMessages) == 0:
                        break

                    for sqsMessage in sqsMessages:
                        try:
                            sqsMessageBody = json.loads(sqsMessage.body).get("Message")
                            data = json.loads(sqsMessageBody)
                            sqsJobId = data.get("job_id")
                        except json.JSONDecodeError as e:
                            print(f"Invalid JSON: {e}")
                            try:
                                sqsMessage.delete()
                            except ClientError as e:
                                print(f"Delete message failed: {e}")
                            continue
                        except AttributeError as e:
                            print(f"AttributeError: {e}")
                            try:
                                sqsMessage.delete()
                            except ClientError as e:
                                print(f"Delete message failed: {e}")
                            continue
                        except Exception as e:
                            print(str(e))
                            try:
                                sqsMessage.delete()
                            except ClientError as e:
                                print(f"Delete message failed: {e}")
                            continue

                        if sqsJobId == jobId:
                            try:
                                sqsMessage.delete()
                            except ClientError as e:
                                print(f"Delete message failed: {e}")
                            except Exception as e:
                                print(str(e))
                            findSqsMessage = True
                            break

                return (
                    jsonify({"code": 201, "message": "Annotation job request processed."}),
                    201,
                )

        return (
            jsonify({"code": 500, "message": "Invalid message type"}),
            500,
        )


# if __name__ == '__main__':
#     app.run(host="0.0.0.0", debug=True)

### EOF
