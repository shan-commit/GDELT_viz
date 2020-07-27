import http.client
import os
import io
import zipfile
import gzip
import boto3
import json
import random

# engine = create_engine('postgresql://postgres:12345678@cse6242.cvgwn66z5x22.us-east-1.rds.amazonaws.com:5432/cse6242')

lambda_client = boto3.client('lambda')
s3 = boto3.client('s3')

bucket_name = 'cse6242-project-data'
def schedule_lambda_for_path(args, context):
    lambda_client.invoke(InvocationType='Event',
                         Payload=json.dumps(args),
                         FunctionName=context.function_name)
    print("Scheduled download for %s" % path)

def lambda_handler(event, context):
    if 'path' not in event.keys():
        return "No path provided"
    if 'target_name' not in event.keys():
        return "No target_name provided"
    path = event['path']
    target_name = event['target_name']
    random_name = 'random_name-%s.csv.gz' % random.randint(0, 1000000)

    file_list = s3.list_objects_v2(
        Bucket=bucket_name,
        Delimiter='/',
        Prefix=path
    )
    buf = bytearray()
    try:
        target_resp = s3.get_object(Bucket=bucket_name, Key=target_name)
        target_buf = gzip.decompress(target_resp['Body'].read())
        buf += gzip.compress(target_buf)
        target_buf = None
    except:
        pass
    i = 0
    for f in file_list['Contents']:
        if context.get_remaining_time_in_millis() < 60000:
            schedule_lambda_for_path(dict(path=path))
            return "To be continued"

        if f['Key'].endswith(target_name):
            continue
        if not f['Key'].lower().endswith('.csv.gz'):
            continue
        resp = s3.get_object(Bucket=bucket_name, Key=f['Key'])
        data = gzip.decompress(resp['Body'].read())
        buf += gzip.compress(data)
        i += 1
        print(i)
    print("Done buf len = %s", len(buf))
    with open("local.csv.gz", 'wb') as o:
        o.write(buf)
    s3.put_object(Bucket=bucket_name, Key=target_name, Body=buf)
    print("Uploaded")


class MyLambdaContext:

    def __init__(self):
        # self.function_name = 'prd_reindexer'
        self.function_name = 'Fake'

    def get_remaining_time_in_millis(self):
        return 1000000000

class MyLambdaClient:

    def invoke(self, InvocationType=None, Payload=None, FunctionName=None):
        pass
        # return
        lambda_handler(json.loads(Payload), MyLambdaContext())

if __name__ == "__main__":
    lambda_client = MyLambdaClient()
    lambda_handler({'pattern': '/202003'}, MyLambdaContext())





class MyLambdaContext:

    def __init__(self):
        # self.function_name = 'prd_reindexer'
        self.function_name = 'Fake'

    def get_remaining_time_in_millis(self):
        return 1000000000

class MyLambdaClient:

    def invoke(self, InvocationType=None, Payload=None, FunctionName=None):
        pass
        # return
        lambda_handler(json.loads(Payload), MyLambdaContext())

if __name__ == "__main__":
    lambda_client = MyLambdaClient()
    lambda_handler({'path': '202003.csv/', 'target_name': '202003.csv/202003.csv.gz'}, MyLambdaContext())

