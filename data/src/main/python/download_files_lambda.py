import http.client
import os
import io
import zipfile
import gzip
import boto3
import json

# engine = create_engine('postgresql://postgres:12345678@cse6242.cvgwn66z5x22.us-east-1.rds.amazonaws.com:5432/cse6242')

file_list_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
file_list_host = "data.gdeltproject.org"
file_list_path = "/gdeltv2/masterfilelist.txt"

lambda_client = boto3.client('lambda')
s3 = boto3.client('s3')

bucket_name = 'cse6242-project-data'
dir_path = "/tmp/data"
dir_export = "%s/export" % dir_path
dir_gkg = "%s/gkg" % dir_path
dir_mentions = "%s/mentions" % dir_path

s3_root_path = "before2018"

export_names = ["globaleventid","day","monthyear","year","fractiondate","actor1code","actor1name","actor1countrycode","actor1knowngroupcode","actor1ethniccode","actor1religion1code","actor1religion2code","actor1type1code","actor1type2code","actor1type3code","actor2code","actor2name","actor2countrycode","actor2knowngroupcode","actor2ethniccode","actor2religion1code","actor2religion2code","actor2type1code","actor2type2code","actor2type3code","isrootevent","eventcode","eventbasecode","eventrootcode","quadclass","goldsteinscale","nummentions","numsources","numarticles","avgtone","actor1geo_type","actor1geo_fullname","actor1geo_countrycode","actor1geo_adm1code","actor1geo_adm2code","actor1geo_lat","actor1geo_long","actor1geo_featureid","actor2geo_type","actor2geo_fullname","actor2geo_countrycode","actor2geo_adm1code","actor2geo_adm2code","actor2geo_lat","actor2geo_long","actor2geo_featureid","actiongeo_type","actiongeo_fullname","actiongeo_countrycode","actiongeo_adm1code","actiongeo_adm2code","actiongeo_lat","actiongeo_long","actiongeo_featureid","dateadded","sourceurl"]
mentions_names = ["globaleventid","eventtimedate","mentiontimedate","mentiontype","mentionsourcename","mentionidentifier","sentenceid","actor1charoffset","actor2charoffset","actioncharoffset","inrawtext","confidence","mentiondoclen","mentiondoctone","mentiondoctranslationinfo","extras"]


try:
    os.stat(dir_path)
except FileNotFoundError as e:
    os.mkdir(dir_path)
    os.mkdir(dir_export)
    os.mkdir(dir_gkg)
    os.mkdir(dir_mentions)


def get_connection():
    return http.client.HTTPConnection(
        file_list_host,
        port=80,
        timeout=40
    )

def schedule_lambda_for_path(path, context):
    lambda_client.invoke(InvocationType='Event',
                         Payload=json.dumps(dict(path=path)),
                         FunctionName=context.function_name)
    print("Scheduled download for %s" % path)

def schedule_lambda_for_paths(paths, context):
    lambda_client.invoke(InvocationType='Event',
                         Payload=json.dumps(dict(paths=paths)),
                         FunctionName=context.function_name)
    print("Scheduled download for %s" % paths)

def download_event(data_file_url):
    print("Downloading %s..." % data_file_url)

    data_file_url_path = data_file_url.split(file_list_host)[1]
    file_name = data_file_url.rsplit("/", 1)[1]
    if '.gkg.' in file_name:
        return False

    download_conn = get_connection()
    try:
        download_conn.request('GET', data_file_url_path)
        resp = download_conn.getresponse()
        if resp.code != 200:
            if resp.code == 404:
                print("Warning: file is not exists %s" % data_file_url_path)
                return False
            raise Exception("Cannot load %s" % (data_file_url))
        if '.export.' in file_name:
            dir_base_path = s3_root_path + "/" + "events"
        else:
            dir_base_path = s3_root_path + "/" + "mentions"

        zip_obj = io.BytesIO(resp.read())

        with zipfile.ZipFile(zip_obj, mode="r") as zip_file:
            zip_file.read(zip_file.namelist()[0])
            key = dir_base_path + "/" + file_name[:-4] + ".gz"
            # key -= ".zip"
            os.makedirs("/Users/istomins/tmp/data5/gzip/" + dir_base_path + "/", exist_ok=True)
            with open("/Users/istomins/tmp/data5/gzip/" + key, "wb") as o:
                o.write(gzip.compress(zip_file.read(zip_file.namelist()[0])))
            # s3.put_object(Key=key, Bucket=bucket_name, Body=gzip.compress(zip_file.read(zip_file.namelist()[0])))

    finally:
        download_conn.close()

    return True

def lambda_handler(event, context):
    if event.get('s3_root_path'):
        s3_root_path = event.get('s3_root_path')
    if 'path' in event.keys():
        download_event(event['path'])
        return "Downloaded %s" % event['path']

    if 'paths' in event.keys():
        for path in event['paths']:
            download_event(path)
        return "Downloaded %s" % event['paths']

    data_list_file = "/tmp/masterfilelist.txt"

    try:
        os.stat(data_list_file)
    except FileNotFoundError:
        conn = get_connection()
        try:
            conn.request('GET', file_list_path)
            resp = conn.getresponse()
            if resp.code != 200:
                raise Exception("Cannot load %s" % (file_list_url))
            with open(data_list_file, "wb") as f:
                f.write(resp.read())
        finally:
            conn.close()


    with open(data_list_file, "rb") as resp:
        lines = filter(lambda l: event['pattern'] in l, map(lambda l: str(l, 'utf-8'), resp.readlines()))
        buf = []
        for line in lines:
            parts = line.rsplit(" ", 1)
            data_file_url = parts[1].strip()
            buf.append(data_file_url)
            if len(buf) == 150:
                schedule_lambda_for_paths(buf, context)
                buf = []
        if len(buf) > 0:
            schedule_lambda_for_paths(buf, context)

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

