import json
from botocore.exceptions import ClientError
import logging
import os
import datetime
import boto3
from urllib import parse


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Enable Debug Logging
# boto3.set_stream_logger("")


# Define Environmental Variables
my_glue_db = str(os.environ['glue_db'])
my_glue_tbl = str(os.environ['glue_tbl'])
my_workgroup_name = str(os.environ['workgroup_name'])
my_s3_bucket = str(os.environ['s3_bucket'])
num_days = int(os.environ['obj_min_retention'])

## Define Date Time and Min Retention Date
safety_margin = int(os.environ['obj_safety_margin'])

my_current_date = datetime.datetime.now().date()
retain_date = datetime.datetime.now().date() + datetime.timedelta(num_days) + datetime.timedelta(safety_margin)

logger.info(f'my_current_date is: {my_current_date}')
logger.info(f'my_retain_date is: {retain_date}')


# Set Service Client
athena_client = boto3.client('athena')


def start_query_execution(query_string, athena_db, workgroup_name, job_request_token):
    logger.info(f'Starting Athena query...... with query string: {query_string}')
    try:
        execute_query = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': athena_db
            },
            WorkGroup=workgroup_name,
            ClientRequestToken= job_request_token,
        )
    except ClientError as e:
        logger.info(e)
    else:
        logger.info(f'Query Successful: {execute_query}')


def lambda_handler(event, context):
    logger.info(event)
    # Use sequencer to prevent duplicate invocation
    my_event_sequencer = str(event['Records'][0]['s3']['object']['sequencer'])
    my_request_token =  my_event_sequencer + '-' + my_event_sequencer + '-' + my_event_sequencer
    # Construct dt partition string
    my_dt = parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8').split('/')[-2].split('=')[-1]
    logger.info(f'Initiating Main Function...')
    # Specify the Athena Query #
    my_query_string = f"""
    SELECT DISTINCT bucket as "{my_s3_bucket}", key as "my_key"
    FROM "{my_glue_db}"."{my_glue_tbl}"
    WHERE dt = '{my_dt}'
    AND
    (object_lock_retain_until_date <= cast('{retain_date}' as timestamp) OR object_lock_mode IS NULL OR object_lock_mode != 'COMPLIANCE' ) ;
    """
    try:
        start_query_execution(my_query_string, my_glue_db, my_workgroup_name, my_request_token)
    except Exception as e:
        logger.error(e)
