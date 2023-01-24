import json
import boto3
import logging

logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    # TODO implement
    #To implement a backup system for latest uploaded file into the s3 bucket
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    logger.setLevel(logging.DEBUG)
    logger.info('Setting the source bucket')
    bucket = 'sourcebucket252'
    logger.info('Setting the destination bucket')
    destination_bucket = 'destinationbucket252'
    latest_files=[]
    time_stamps = []
    logger.info('getting all the objects info in source bucket')
    response = s3_client.list_objects_v2(Bucket = bucket)
    for obj in response['Contents']:
        #print(obj['Key']+" Last Modified "+str(obj["LastModified"]))
        latest_files.append(obj['Key'] + ","+str(obj["LastModified"]))
        time_stamps.append(str(obj["LastModified"]))
    
    time_stamps.sort()
    #for i in range(0,len(time_stamps)):
        #print(time_stamps[i])
    logger.info('Obtaing lastest time_stamp')
    get_lastest_timestamp = str(time_stamps[len(time_stamps)-1])
    res = [i for i in latest_files if get_lastest_timestamp in i]
    get_latest_file = str(res[0]).split(",")[0]
    print("Lastest File in S3 is "+ get_latest_file)
    
    copy_source = {
      'Bucket': bucket,
      'Key': get_latest_file
    }
    logger.info('Copying the file to the destination bucket')
    bucket = s3_resource.Bucket(destination_bucket)
    bucket.copy(copy_source, "backup_"+get_latest_file)
    logger.info('File backup has been done')
    
    
        
    
        