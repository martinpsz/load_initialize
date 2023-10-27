from dotenv import dotenv_values
import boto3
import re
import json
import pandas as pd
import tempfile
import gzip
import csv


#Connect to AWS S3 bucket with a new boto session
config = dict(dotenv_values(".env"))

#Create a new session. Put env file in cwd with these params.
session = boto3.session.Session(
    aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
    aws_session_token=config["AWS_SESSION_TOKEN"],
    region_name=config["AWS_REGION"],
    profile_name='afscme-development'
)

#Connect to and create local file list that matches 'enterprise'
#Uncomment and run if you don't have a local file with the bucket list already.
SOURCE_DIR = 'event-distrib-dev-acct-test-archive-b0842d1'

s3_client = session.resource('s3')
source_bucket = s3_client.Bucket(SOURCE_DIR)

'''
file_type_regex = re.compile(r'enterprise_(.*)(?=_\d)')

enterprise_event_links = {}
event_types = []
for items in source_bucket.objects.filter(Prefix='prod-data/load-2023-10-13/'):
    regexMatch = re.search(file_type_regex, items.key)

    if regexMatch:
        event_type = regexMatch.group()

        if event_type not in enterprise_event_links:
            enterprise_event_links[event_type] = []

        enterprise_event_links[event_type].append(items.key)
    
json_object = json.dumps(enterprise_event_links, indent=4)
with open('prod_data_links.json', 'w') as f:
    f.write(json_object)
'''

with open('prod_data_links.json') as f:
    event_links = json.load(f)


#for k, v in event_links.items():
#    print(k, v[0])




def retrieveAndSaveHeaders(bucket, file, event_name):

    with tempfile.NamedTemporaryFile() as f: 
        bucket.download_fileobj(file, f)

        cols = []
        with gzip.open(f.name, 'rb') as g:
            df = pd.read_json(g, lines=True)
            data = pd.json_normalize(df['data'])

            cols.append(data.columns.to_list())
            cols[0].append('ts')

            with open(f"{event_name}_headers.tsv", 'w') as h:
                writer = csv.writer(h, delimiter='\t')

                for col in cols:
                    writer.writerow(col)

        f.close()

#for k, v in event_links.items():
#    retrieveAndSaveHeaders(source_bucket, v[0], k)


def returnUniqueAndLatestEventData():
    test_file = event_links['enterprise_aff_organization'][0]

    hashMap = {}
    with tempfile.NamedTemporaryFile() as f:
        source_bucket.download_fileobj(test_file, f)

        with gzip.open(f.name, 'rb') as g:
            df = pd.read_json(g, lines=True)
            data = pd.json_normalize(df['data'])

            timestamp = df['ts']
            primary_key = data['aff_pk'] 

            

            #if primary_key in hashMap:
            #    if timestamp > hashMap[primary_key][1]:
            #        hashMap[primary_key] = (data, timestamp)
            #else:
            #    hashMap[primary_key] = (data, timestamp)
    
    #print(hashMap)


returnUniqueAndLatestEventData()
#Get list of headings and save to file for Viktoriya
#Deduplication process
#Generate tsv files