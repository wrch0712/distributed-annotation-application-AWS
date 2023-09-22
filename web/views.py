# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##

import uuid
import time
import json
import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import abort, flash, redirect, render_template, request, session, url_for

from app import app, db
from decorators import authenticated, is_premium

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    # Generate unique ID to be used as S3 key (name)
    key_name = (
        app.config["AWS_S3_KEY_PREFIX"]
        + user_id
        + "/"
        + str(uuid.uuid4())
        + "~${filename}"
    )

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html", s3_post=presigned_post, role=session["role"]
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
@authenticated
def create_annotation_job_request():

    region = app.config["AWS_REGION_NAME"]

    # Parse redirect URL query parameters for S3 object info
    bucket_name = request.args.get("bucket")
    s3_key = request.args.get("key")

    if not bucket_name or not s3_key:
        app.logger.error("Cannot find bucket and/or key in request arguments")
        return abort(500)

    # Extract the job ID from the S3 key
    # Move your code here
    job_id = s3_key.split("/")[-1].split("~")[0]
    user_id = s3_key.split("/")[-2]
    input_file_name = s3_key.split("/")[-1].split("~")[-1]
    if job_id == "" or user_id == "" or input_file_name == "":
        app.logger.error("Cannot get correct job ID or input file name")
        return abort(500)

    # Persist job to database
    # Move your code here...
    data = {
        "job_id": job_id,
        "user_id": user_id,
        "input_file_name": input_file_name,
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": s3_key,
        "submit_time": int(time.time()),
        "job_status": "PENDING"
    }

    dynamodb = boto3.resource('dynamodb', region_name=region)
    try:
        table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
        table.put_item(Item=data)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            app.logger.error(f"DynamoDB table not found: {e}")
            return abort(500)
        else:
            app.logger.error(f"ClientError: {e}")
            return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    # Send message to request queue
    # Move your code here...
    sns = boto3.resource("sns", region_name=region)
    topic = sns.Topic(app.config["AWS_SNS_JOB_REQUEST_TOPIC"])
    try:
        topic.publish(Message=json.dumps(data))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NotFound":
            app.logger.error(f"SNS topic not found: {e}")
            return abort(500)
        elif e.response["Error"]["Code"] == "InvalidParameter":
            app.logger.error(f"Invalid parameter for publish: {e}")
            return abort(500)
        else:
            app.logger.error(f"ClientError: {e}")
            return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    return render_template("annotate_confirm.html", job_id=job_id)


"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():

    # Get list of annotations to display
    try:
        user_id = session["primary_identity"]
    except KeyError as e:
        app.logger.error(f"primary_identity does not exit: {e}")
        return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    # dynamodb query
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
    dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
    try:
        table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
        response = table.query(IndexName=app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE_SECONDARY_INDEX"],
                               KeyConditionExpression="user_id = :uid",
                               ExpressionAttributeValues={":uid": user_id},
                               ProjectionExpression="job_id, input_file_name, job_status, submit_time")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            app.logger.error(f"DynamoDB table not found: {e}")
            return abort(500)
        else:
            app.logger.error(f"ClientError: {e}")
            return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    annotations = []
    for item in response["Items"]:
        try:
            job_id = item["job_id"]
            input_file_name = item["input_file_name"]
            job_status = item["job_status"]
            epoch_time = item["submit_time"]
        except KeyError as e:
            app.logger.error(f"Miss value in the dynamodb annotation record: {e}")
            continue
        except Exception as e:
            app.logger.error(str(e))
            continue

        # convert epoch submit time to CST date time
        # https://stackoverflow.com/questions/12978391/localizing-epoch-time-with-pytz-in-python
        try:
            dt = datetime.datetime.fromtimestamp(int(epoch_time)) # convert timestamp to datetime
            time = dt.strftime("%Y-%m-%d %H:%M") # format as string
        except ValueError as e:
            # if the time conversion failed, just display the epoch time and log the error, so that it won't ruin the whole page
            time = epoch_time
            app.logger.error(f"Error happened wen cast submit epoch time to CST date time: {e}")

        annotations.append({
            "job_id": job_id,
            "submit_time": time,
            "input_file_name": input_file_name,
            "job_status": job_status
        })

    return render_template("annotations.html", annotations=annotations)


"""Display details of a specific annotation job
"""


@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):

    try:
        user_id = session["primary_identity"]
    except KeyError as e:
        app.logger.error(f"primary_identity does not exit: {e}")
        return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    # dynamodb query
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
    dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
    try:
        table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
        response = table.query(KeyConditionExpression="job_id = :jid",
                               ExpressionAttributeValues={":jid": id})
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            app.logger.error(f"DynamoDB table not found: {e}")
            return abort(500)
        else:
            app.logger.error(f"ClientError: {e}")
            return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    # Returning 404 page for job not found
    if len(response["Items"]) == 0:
        app.logger.error(f"Failed to get job info from DynamoDB")
        return abort(404, "Job id not found")

    if len(response["Items"]) > 1:
        app.logger.error(f"Failed to get job info from DynamoDB")
        return abort(500)

    item = response["Items"][0]
    try:
        job_user_id = item["user_id"]
        status = item["job_status"]
        request_epoch_time = item["submit_time"]
        input_file_name = item["input_file_name"]
        input_bucket = item["s3_inputs_bucket"]
        input_file_s3_key = item["s3_key_input_file"]
    except KeyError as e:
        app.logger.error(f"Miss value in the dynamodb annotation record: {e}")
        return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    # Check authorization
    if job_user_id != user_id:
        app.logger.error(f"Authorization Failed")
        return abort(403, "Your are unauthorized to view the job information")

    # convert epoch submit time to CST date time
    # https://stackoverflow.com/questions/12978391/localizing-epoch-time-with-pytz-in-python
    try:
        request_dt = datetime.datetime.fromtimestamp(int(request_epoch_time))  # convert timestamp to datetime
        request_time = request_dt.strftime("%Y-%m-%d @ %H:%M:%S")  # format as string
    except ValueError as e:
        # if the time conversion failed, just display the epoch time and log the error, so that it won't ruin the whole page
        request_time = request_epoch_time
        app.logger.error(f"Error happened wen cast submit epoch time to CST date time: {e}")

    # Generate presigned url to download results file from s3
    # https://allwin-raju-12.medium.com/boto3-and-python-upload-download-generate-pre-signed-urls-and-delete-files-from-the-bucket-87b959f7bbaf
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
    s3 = boto3.client('s3')
    try:
        input_download_url = s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": input_bucket,
                "Key": input_file_s3_key
            },
            ExpiresIn=app.config["AWS_S3_SIGNED_DOWNLOAD_EXPIRATION"]
            # I set the expiration time of s3 presigned download url to be 60s and made this variable in config.py
        )
    except ClientError as e:
        app.logger.error(f"Failed to generate pre-signed download URL for result file: {e}")
        return abort(500)

    if status == "PENDING" or status == "RUNNING":
        job_info = {
            "request_id": id,
            "request_time": request_time,
            "status": status,
            "input_filename": input_file_name,
            "input_download_url": input_download_url
        }
    elif status == "COMPLETED":
        try:
            completion_epoch_time = item["completion_time"]
            result_file_s3_key = item["s3_key_result_file"]
            result_bucket = item["s3_results_bucket"]
        except KeyError as e:
            app.logger.error(f"Miss value in the dynamodb annotation record: {e}")
            return abort(500)
        except Exception as e:
            app.logger.error(str(e))
            return abort(500)

        try:
            completion_dt = datetime.datetime.fromtimestamp(int(completion_epoch_time))  # convert timestamp to datetime
            completion_time = completion_dt.strftime("%Y-%m-%d @ %H:%M:%S")  # format as string
        except ValueError as e:
            # if the time conversion failed, just display the epoch time and log the error, so that it won't ruin the whole page
            completion_time = completion_epoch_time
            app.logger.error(f"Error happened wen cast submit epoch time to CST date time: {e}")

        # If the user is free user and the job has already been archived
        if "results_file_archive_id" in item:
            job_info = {
                "request_id": id,
                "request_time": request_time,
                "status": status,
                "input_filename": input_file_name,
                "input_download_url": input_download_url,
                "complete_time": completion_time
            }
        else:
            # Generate presigned url to download results file from s3
            # https://allwin-raju-12.medium.com/boto3-and-python-upload-download-generate-pre-signed-urls-and-delete-files-from-the-bucket-87b959f7bbaf
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
            try:
                result_download_url = s3.generate_presigned_url(
                    "get_object",
                    Params={
                        "Bucket": result_bucket,
                        "Key": result_file_s3_key
                    },
                    ExpiresIn=app.config["AWS_S3_SIGNED_DOWNLOAD_EXPIRATION"]
                    # I set the expiration time of s3 presigned download url to be 60s and made this variable in config.py
                )
            except ClientError as e:
                app.logger.error(f"Failed to generate pre-signed download URL for result file: {e}")
                abort(500)

            # Returning 404 page for results file not found
            if result_download_url is None:
                app.logger.error(f"Result file not found in S3: {result_file_s3_key}")
                return abort(404)

            job_info = {
                "request_id": id,
                "request_time": request_time,
                "status": status,
                "input_filename": input_file_name,
                "input_download_url": input_download_url,
                "complete_time": completion_time,
                "result_download_url": result_download_url
            }
    else:
        app.logger.error(f"Invalid value of status")
        return abort(500)

    return render_template('annotation.html', job_info=job_info)


"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):
    try:
        user_id = session["primary_identity"]
    except KeyError as e:
        app.logger.error(f"primary_identity does not exit: {e}")
        return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    # dynamodb query
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
    dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
    try:
        table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
        response = table.query(KeyConditionExpression="job_id = :jid",
                               ExpressionAttributeValues={":jid": id},
                               ProjectionExpression="user_id, job_status, s3_results_bucket, s3_key_log_file")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            app.logger.error(f"DynamoDB table not found: {e}")
            return abort(404)
        else:
            app.logger.error(f"ClientError: {e}")
            return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    # Returning 404 page for job not found
    if len(response["Items"]) == 0:
        app.logger.error(f"Failed to get job info from DynamoDB")
        return abort(404, "Job id not found")

    if len(response["Items"]) > 1:
        app.logger.error(f"Failed to get job info from DynamoDB")
        return abort(500)

    item = response["Items"][0]
    try:
        job_user_id = item["user_id"]
        status = item["job_status"]
    except KeyError as e:
        app.logger.error(f"Miss value in the dynamodb annotation record: {e}")
        return abort(500)
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)

    # Check authorization
    if job_user_id != user_id:
        app.logger.error(f"Authorization Failed")
        return abort(403, "Your are unauthorized to view the job information")

    if status != "COMPLETED":
        app.logger.error(f"Annotation of the job has not completed yet")
        return abort(404, "Annotation of the job has not completed yet.")

    try:
        result_bucket = item["s3_results_bucket"]
        log_file_s3_key = item["s3_key_log_file"]
    except KeyError as e:
        app.logger.error(f"Miss value in the dynamodb annotation record: {e}")
        return abort(404)  # Returning 404 page for log file not found
    except Exception as e:
        app.logger.error(str(e))
        return abort(500)
    
    # Read log file in S3 into a string
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/object/index.html
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/object/get.html
    # https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3

    s3 = boto3.resource("s3")
    try:
        log_object = s3.Object(result_bucket, log_file_s3_key)
        log_contents = log_object.get()["Body"].read().decode("utf-8")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            app.logger.error(f"Log file not found in S3: {e}")
            return abort(404) # Returning 404 page for log file not found
        else:
            app.logger.error(f"Failed to read log file from S3: {e}")
            return abort(500)

    return render_template("view_log.html", job_id=id, log_contents=log_contents)


"""Subscription management handler
"""
import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
@authenticated
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info
        return render_template("subscribe.html")

    elif request.method == "POST":
        # Process the subscription request
        stripe_token = request.form.get("stripe_token")

        # Create a customer on Stripe
        # https://stripe.com/docs/api/customers/create?lang=python
        # https://stripe.com/docs/api/errors
        try:
            customer = stripe.Customer.create(
                source=stripe_token,
                email=session["email"],
                name=session["name"],
                api_key=app.config["STRIPE_SECRET_KEY"]
            )
        except stripe.error.StripeError as e:
            app.logger.error(f"Failed to create customer on Stripe: {e}")
            return abort(500)

        # Subscribe customer to pricing plan
        # https://stripe.com/docs/api/subscriptions/create?lang=python
        try:
            stripe.Subscription.create(
                customer=customer.id,
                items=[{"price": app.config["STRIPE_PRICE_ID"]}],
                api_key=app.config["STRIPE_SECRET_KEY"]
            )
        except stripe.error.StripeError as e:
            app.logger.error(f"Failed to subscribe customer to pricing plan: {e}")
            return abort(500)

        # Update user role in accounts database
        user_id = session["primary_identity"]
        try:
            update_profile(identity_id=user_id, role="premium_user")
        except Exception as e:
            app.logger.error(f"Failed to update user role in accounts database: {e}")
            return abort(500)

        # Update role in the session
        session["role"] = "premium_user"

        # Request restoration of the user's data from Glacier
        # ...add code here to initiate restoration of archived user data
        # get archive results file archive id of the user
        dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
        try:
            table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
            response = table.query(IndexName=app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE_SECONDARY_INDEX"],
                                   KeyConditionExpression="user_id = :uid",
                                   ExpressionAttributeValues={":uid": user_id},
                                   ProjectionExpression="job_id, s3_results_bucket, s3_key_result_file, results_file_archive_id")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                app.logger.error(f"DynamoDB table not found: {e}")
                return abort(500)
            else:
                app.logger.error(f"ClientError: {e}")
                return abort(500)
        except Exception as e:
            app.logger.error(str(e))
            return abort(500)

        # send thaw sns
        sns = boto3.resource("sns", region_name=app.config["AWS_REGION_NAME"])
        topic = sns.Topic(app.config["AWS_SNS_JOB_THAW_TOPIC"])
        for item in response["Items"]:
            if "results_file_archive_id" not in item:
                continue
            try:
                job_id = item["job_id"]
                s3_results_bucket = item["s3_results_bucket"]
                result_file_s3_key = item["s3_key_result_file"]
                results_file_archive_id = item["results_file_archive_id"]
            except KeyError as e:
                app.logger.error(f"Miss value in the dynamodb annotation record: {e}")
                continue
            thaw_message = {
                "job_id": job_id,
                "s3_results_bucket": s3_results_bucket,
                "result_file_s3_key": result_file_s3_key,
                "results_file_archive_id": results_file_archive_id
            }
            try:
                topic.publish(Message=json.dumps(thaw_message))
            except ClientError as e:
                if e.response["Error"]["Code"] == "NotFound":
                    app.logger.error(f"SNS topic not found: {e}")
                elif e.response["Error"]["Code"] == "InvalidParameter":
                    app.logger.error(f"Invalid parameter for publish: {e}")
                else:
                    app.logger.error(f"ClientError: {e}")
            except Exception as e:
                app.logger.error(str(e))

        # ...and make sure you handle files pending archive!
        # I handle files pending archive in ann/run.py
        # When run.py finishes annotation, it will also check the user’s role and call the state machine if it’s a free user.
        # The state machine will wait for 5 minutes and call a lambda function to send a job archive notification to the job archive queue.
        # Before sending the job archive notification, the lambda function will check user's role again

        # Display confirmation page
        return render_template("subscribe_confirm.html", stripe_id=customer.id)


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF
