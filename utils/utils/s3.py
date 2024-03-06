import os
import boto3
import logging


logger = logging.getLogger(__name__)

class BucketPath:

    def __init__(self, bucket_name, bucket_prefix):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.bucket_prefix = bucket_prefix
    

    @property
    def keys_list(self):
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name
            ,Prefix=self.bucket_prefix)
        
        objs = response.get('Contents', [])
        return [obj['Key'] for obj in objs]


    def store(self, body: bytes, key:str) -> str:
        self.s3_client.put_object(Body=body, Bucket=self.bucket_name, Key=key)
        s3_uri = os.path.join('s3://', self.bucket_name, key)
        logger.info('File stored in s3.')
        return s3_uri
    

    def vacuum(self):
        logger.info('Cleaning old files.')
        keys = self.keys_list

        if len(keys) > 0:
            logger.info(f"Deleting the following files: {str(keys)}")
            keys_dicts = [{'Key': k} for k in keys]
            Delete = {'Objects': keys_dicts}
            self.s3_client.delete_objects(Bucket=self.bucket_name, Delete=Delete)
        else:
            logger.info('No file to delete.')