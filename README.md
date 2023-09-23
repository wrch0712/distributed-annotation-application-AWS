# Distributed Annotation Application AWS

A DNA annotation application based on AWS, Python Boto3 API and Flask web framework.

AWS porducts used: EC2, S3, Glacier, DynamoDB, SNS, SQS, Lambda, Step Function

AWS account info is hidden.

## Project Structure
Directory contents are as follows:
* `/web` - The web app files
* `/ann` - Annotator files
* `/util` - Utility scripts/apps for notifications, archival, and restoration
* `/aws` - AWS user data files

## Workflow
### Submit job
When an annotation job is submitted in the web server, the web server sends a job request notification to the job requests queue. The annotation server will poll the request queue and deal with the request. When run.py finishes annotation, it will send a job result notification to the job results queue, which is polled by the util server, the notified.py in util server deals with the job result and sends an email to the user indicating the job is completed. When run.py finishes annotation, it will also check the user’s role and call the state machine if it’s a free user. The state machine will wait for 5 minutes and send a job archive notification to the job archive queue. The archive_script.py in util server will poll the archive queue, get the archive message, check the user’s role again and archive the result file from S3 to Glacier if it’s a free user, update the results file archive id to annotation DynamoDB table. The web server is responsible for displaying the job detail, it will get information from the annotation DynamoDB table, displaying different information according to different job status. If the user is a free user and the results file archive id can be found in the annotation DynamoDB table, the web server won’t generate result file download url and display to user.

### User Upgrade:
When a user subscribes, the web server will collect job information of this user from the annotation DynamoDB table, and send a thawing notification for each archived annotation (in /subscribe route). The thaw SNS sends messages to the thaw SQS directly. The thaw_script.py in the util server polls the thaw queue, and initiates an archive-retrieval job for each thaw message. It first attempts to initiate an expedited retrieval and uses a standard retrieval if it fails. A restore notification will be sent once the thawing is completed (this is done by setting SNSTopic parameter in Glacier.Client.initiate_job method). The restore SNS sends messages to the restore SQS directly, and the restore SQS will trigger a restore Lambda Function ( add SQS as a trigger of Lambda Function). The restore Lambda Function will copy the temporary thawed object to the S3 gas-results bucket, delete the archived object from Glacier, update the annotation DynamoDB table, and delete the message in restore SQS after processing. The web server is responsible for displaying the job detail, it will get information from the annotation DynamoDB table, displaying different information according to different job status. It displays different information for result file based on user’s role and whether the results file archive id can be found in the annotation DynamoDB table.

## choice of AWS services and reasons

### Step Function to implement the waiting
I chose Step Function to implement the waiting before sending the archive message. Step Functions is a fully managed service that allows the coordination and orchestrate multiple AWS services using visual workflows. I chose it for the following reasons. 
1. Step Functions is a serverless orchestration service, it automatically scales to handle the load and concurrency requirements of state machine executions. Whether we have a small number of state machine executions or thousands running concurrently, the Wait state can handle the scalability needs seamlessly.
2. Step Functions provides built-in support for waiting states, and allowing specify a delay before proceeding to the next step in the workflow. The Wait state in Step Functions provides consistent behavior across executions. When a state machine enters a Wait state, the execution is effectively paused until the specified duration or timestamp is reached. The state machine retains its current state and context during the waiting period, ensuring consistency when it resumes.
3. The Wait state in Step Functions can deal with waiting multiple scenarios. It can handle very short wait durations, such as a few seconds, with high precision and accuracy. It also allows for wait durations of up to one year, enabling you to implement long delays if needed.
4. If a state machine execution is interrupted, the Wait state ensures that the execution resumes from the exact point it left off once it's restarted. The Wait state ensures the desired wait period is completed before transitioning to the next state.
5. For the cost, ts current state and context during the waiting period, ensuring consistency when it resumes.

### SNS and SQS for thawing, SNS, SQS and Lambda Function for restoration.
I chose the design above for the following reasons.
1. Decoupling restoration through message queues and notification topics enhances fault isolation. If any component of the restoration process fails or experiences issues, it won't directly impact other GAS tasks. Failed restoration requests can be retried or handled separately, ensuring the overall system remains resilient and unaffected.
2. By using SNS and SQS together ensures reliable message delivery and provides fault tolerance. If a Lambda Function fails to process a message, it remains in the queue and can be retried or processed by another instance of the Lambda Function.
3. SQS provides a highly scalable and distributed message queue that can handle many incoming messages. As the message queue grows, Lambda can automatically scale to handle the increased workload by invoking multiple instances of the function in parallel. This enables the system to efficiently handle spikes in message traffic without overwhelming the resources or causing performance degradation.
4. SQS queues can be configured as Lambda built-in event sources. Lambda takes over and automates activities such as polling, reading, and removing messages from a queue. Successfully processed messages are discarded, while unsuccessful messages are routed to the DLQ or returned to the queue. These stages do not need to be configured directly inside a Lambda function. It’s convenient to use.
5. Once the thawing process is completed, a restore notification is sent using SNS. This triggers the restore SQS, which in turn triggers a restore Lambda Function. This event-driven approach ensures that restoration happens seamlessly and automatically after thawing, without the need for manual intervention.


