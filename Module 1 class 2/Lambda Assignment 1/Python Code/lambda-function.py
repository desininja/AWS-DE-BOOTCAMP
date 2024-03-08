import logging
import json
import boto3
import os

default_log_args = {
    "level": logging.DEBUG if os.environ.get("DEBUG", False) else logging.INFO,
    "format": "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    "datefmt": "%d-%b-%y %H:%M",
    "force": True,
}
logging.basicConfig(**default_log_args)
log = logging.getLogger("")

def lambda_handler(event, context):

    source_bucket = 'gwd-lambda'
    s3 = boto3.client('s3')

    response_contents = s3.list_objects_v2(Bucket = source_bucket).get('Contents')
    sorted_rc = sorted(response_contents,key = lambda x:x['LastModified'],reverse=True)
    current_uploaded_file = sorted_rc[0]
        
    file_size = round(current_uploaded_file.get('Size')/1024**2,2)
    if file_size > 100:
        log.info(f"Uploaded file size is GREATER than 100 MB")
        log.info(f"File Name: {current_uploaded_file.get('Key')}")
        log.info(f"Size: {file_size}")
    else:
        log.info(f"Uploaded file size is SMALL than 100 MB")
        log.info(f"File Name: {current_uploaded_file.get('Key')}")
        log.info(f"Size of file {file_size}")

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda triggered on file upload')
    }
