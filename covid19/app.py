import os
import json
import boto3
from sodapy import Socrata
from datetime import datetime
from botocore.exceptions import NoCredentialsError
import pytz

def lambda_handler(event, context):
    s3_client= boto3.client('s3')
    s3_resource =  boto3.resource('s3')
    client = Socrata("www.datos.gov.co", None)
    results = client.get_all(os.environ['DATASET'])
    tmp_path="/tmp/"
    file_prefix=os.environ['FILE_PREFIX']
    now = datetime.now(pytz.timezone("America/Bogota"))
    deit=now.strftime("%Y%m%d")
    bucket_path=os.environ['BUCKET_PATH']
    bucket=os.environ['BUCKET']
    local_file='%s%s_%s.json'%(tmp_path,file_prefix,deit)
    s3_file='%s%s_%s.json'%(bucket_path,file_prefix,deit)
    with open(local_file,'w') as f:
        for item in results:
            json.dump(item,f,ensure_ascii=False)
            f.write("\n")
    
    with open (local_file, "r") as myfile:
        data=myfile.readlines()
    vec=len(data)
    s3_client.upload_file(local_file, bucket, s3_file)    
    uploaded=True
    print("Upload Successful from %s to bucket %s in path %s"%(local_file, bucket, s3_file))
    return {
        "statusCode": 200,
        "body": json.dumps({"results":vec,"uploaded":uploaded}),
    }

