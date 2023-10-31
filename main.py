from dotenv import dotenv_values
import boto3
import re
import json
import pandas as pd
import tempfile
import gzip
import csv
import asyncio
import hashlib



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

##################################################################################
def raw_id(event):
    match event['evt']:
        case 'enterprise_aff_member':
            return f"{event['data']['aff_pk']}:{event['data']['person_pk']}" 
        case 'enterprise_person':
            return f"{event['data']['person_pk']}"
        case 'enterprise_aff_organization':
            return f"{event['data']['aff_pk']}"
        case 'enterprise_person_address':
            return f"{event['data']['address_pk']}"
        case 'enterprise_person_phone':
            return f"{event['data']['phone_pk']}"
        case 'enterprise_person_email':
            return f"{event['data']['email_pk']}"
        case 'enterprise_office':
            return f"{event['data']['office_pk']}"
        case 'enterprise_term_of_office':
            return f"{event['data']['term_of_office_pk']}"
        
    #return f"{evt['src_metadata']['table-name']}:{evt['src_metadata']['partition-key-value']}"



async def returnRecords(file, bucket):
    #Read through list of files and return records in each file.
    rec = []
    with tempfile.NamedTemporaryFile() as tf:
        bucket.download_fileobj(file, tf)

        
        with gzip.open(tf.name, 'rb') as gz:
            line = gz.readlines()

            for row in line:
                rec.append(json.loads(row))

        tf.close()

    return rec

async def deduplicateRecords(records):
    d = {}
    for line in records:

        id = raw_id(line)
        payload, payload['ts'] = line['data'], line['ts'] 

        if('id' in d):
            exist = d[id]
                   

            d[id] = payload if line['src_metadata']['timestamp'] > exist['src_metadata']['timestamp'] else exist
        else:
            d[id] = payload

    return d


async def main(links, bucket):
    #for each file in list of files, open file and read in line by line and save the record to 'rec'
    
    for k, v in links.items():
        deduped = {}
        
        for link in v:
            records = await returnRecords(link, bucket)
            deduped.update(await deduplicateRecords(records))


        payload = []
        for _, v in deduped.items():  
            payload.append(v)

        df = pd.DataFrame(payload)
        df.to_csv(f"{k}.tsv", sep='\t', index=False)
        

asyncio.run(main(event_links, source_bucket))

