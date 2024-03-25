import json
import boto3
import os
import cfnresponse
import logging


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Enable Debug Logging
# boto3.set_stream_logger("")            

SUCCESS = "SUCCESS"
FAILED = "FAILED"

# Create Service Client
s3 = boto3.resource('s3')
s3client = boto3.client('s3')

### Define Environmental Variables ###
my_inv_schedule = str(os.environ['inv_report_schedule'])
accountId = str(os.environ['account_id'])
my_config_id = str(os.environ['inv_config_id'])


def config_s3_inventory(src_bucket, config_id, dst_bucket,
                        inv_format, src_prefix, dst_prefix, inv_status, inv_schedule, incl_versions):

    ## Generate default kwargs ##
    my_request_kwargs = {
        'Bucket': src_bucket,
        'Id': config_id,
        'InventoryConfiguration': {
            'Destination': {
                'S3BucketDestination': {
                    # 'AccountId': account_id,
                    'Bucket': f'arn:aws:s3:::{dst_bucket}',
                    'Format': inv_format,
                    'Prefix': dst_prefix,
                    'Encryption': {
                        'SSES3': {}
                    }
                }
            },
            'IsEnabled': inv_status,
            'Filter': {
                'Prefix': src_prefix
            },
            'Id': config_id,
            'IncludedObjectVersions': incl_versions,
            'OptionalFields': [
                'LastModifiedDate',
                'ETag',
                'IsMultipartUploaded',
                'ObjectLockRetainUntilDate',
                'ObjectLockMode',
                'ObjectLockLegalHoldStatus',                            
            ],
            'Schedule': {
                'Frequency': inv_schedule
            }
        }
    }

    ## Remove Prefix Parameter if No Value is Specified, All Bucket ##

    logger.info(src_prefix)
    if src_prefix == '' or src_prefix is None:
        logger.info(f'removing filter parameter')
        my_request_kwargs['InventoryConfiguration'].pop('Filter')
        logger.info(f"Modify kwargs no prefix specified: {my_request_kwargs}")

    # Initiating Actual PutBucket Inventory API Call ##
    try:
        logger.info(f'Applying inventory configuration to S3 bucket {src_bucket}')
        s3client.put_bucket_inventory_configuration(**my_request_kwargs)
    except Exception as e:
        logger.error(f'An error occurred processing, error details are: {e}')


def del_inventory_configuration(src_bucket, config_id):
    try:
        logger.info(f"Starting the process to remove the S3 Inventory configuration {config_id}")
        response = s3client.delete_bucket_inventory_configuration(
            Bucket=src_bucket,
            Id=config_id,
        )
    except Exception as e:
        logger.error(e)
    else:
        logger.info(f"Successfully deleted the S3 Inventory configuration {config_id}")

def lambda_handler(event, context):
    my_incl_versions = 'Current'
    my_inv_format = 'Parquet'
    my_dest_prefix = accountId
    my_inv_status = True
    logger.info(f"Event details: {json.dumps(event, indent=2)}")
    responseData={}
    try:
        if event['RequestType'] == 'Delete':
            logger.info(f"Request Type: {event['RequestType']}")
            logger.info("No Action Required, Inventory deletion is handled by another custom resource!")
            logger.info("Sending response to custom resource after Delete")
        elif event['RequestType'] == 'Create' or event['RequestType'] == 'Update':
            logger.info(f"Request Type: {event['RequestType']}")
            my_src_bucket = event['ResourceProperties']['MyBucketwithObjLock']
            my_src_prefix = event['ResourceProperties']['MyBucketwithObjLockPrefix']
            my_dst_bucket = event['ResourceProperties']['MyS3InventoryDestinationBucket']
            config_s3_inventory(my_src_bucket, my_config_id, my_dst_bucket,
                                    my_inv_format, my_src_prefix, my_dest_prefix, my_inv_status, my_inv_schedule, my_incl_versions)
            logger.info("Sending response to custom resource")
        responseStatus = 'SUCCESS'
        responseData = {'Success': 'Inventory configuration was successfully applied!'}
        cfnresponse.send(event, context, responseStatus, responseData)
    except Exception as e:
        logger.error(f'Failed to process: {e}')
        responseStatus = 'FAILED'
        responseData = {'Failure': e}
        failure_reason = str(e) 
        logger.info("Sending response to custom resource after an Error")
        cfnresponse.send(event, context, responseStatus, responseData, reason=failure_reason)
