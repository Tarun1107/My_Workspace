## DMS task status checker
##----------------------------------------
## Import required packages
import os
import boto3
import logging
##----------------------------------------

##----------------------------------------
## Custom Functions
##----------------------------------------
## Function to get replication_task_status from dms
def dms_replication_task_status(filterdict):
  response = client.describe_replication_tasks(Filters=[filterdict])
  return response

## Function to resume dms replication task
def dms_start_replication_task(task_arn):
  response1 = client.start_replication_task(ReplicationTaskArn = task_arn, StartReplicationTaskType = 'resume-processing')
  return response1

##----------------------------------------
## Initialization
##----------------------------------------
client = boto3.client('dms')
logger = logging.getLogger()

##----------------------------------------
## Environment Variables
##----------------------------------------
number_of_tasks = os.environ['NUMBER_OF_TASKS']

##----------------------------------------
## Lambda Handler code
##----------------------------------------
def lambda_handler(event, context):
  counter = 0
  for i in range(1, int(number_of_tasks)+1):
    counter += 1 #Increment counter
    task_arn = os.environ["TASK_ARN_" + str(counter)] # get task arn from environment variables

    filterdict={}
    filterdict['Name'] = 'replication-task-arn'
    filterdict['Value'] = task_arn

    ## describe_replication_task function call
    response = dms_replictaion_task_status(filterdict)
    status = response['ReplicationTasks'][0]['Status']
    logger.info("status for replication task arn :{0} is :{1}".format(task_arn,status))

    if status == 'failed':
      ErrorMessage = response['ReplicationTasks'][0]['LastFailureMessage']
      logger.info("Error message for replication task arn :{0} is :{1}".format(task_arn,ErrorMessage))
      try:
        response1 = dms_start_replication_task(task_arn)
      except Exception as e:
        logger.info("Exception occured while starting replication task arn :{0}".format(task_arn))
    else:
      logger.info("status for replication task arn :{0} is :{1}".format(task_arn,status))
##----------------------------------------