#Imports
from dotenv import dotenv_values
import boto3
import pandas as pd
import re
from typing import Literal, Optional
import json
import random
import tempfile
import gzip

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

#Constants:
ARCHIVE = 'event-distribution-s3-archive-testing-b1e0c31'
PROD = 'event-distrib-dev-acct-test-archive-b0842d1'
DEST = 'updates-member-card'

#set up connections to all buckets needed.
s3_client = session.resource('s3')
archive_bucket = s3_client.Bucket(f'{ARCHIVE}')
production_bucket = s3_client.Bucket(f'{PROD}')
destination_bucket = s3_client.Bucket(f'{DEST}')

#Regex to filter out events of interest
PERSON_MEMBER_REGEX = re.compile(r"(person_member(?=_))")
PERSON_REGEX = re.compile(r"(enterprise_person(?=_\d))")
ADDRESS_REGEX = re.compile(r"(enterprise_person(?=_address))")
AFF_MEMBER_REGEX = re.compile(r"(enterprise_aff(?=_member))")
AFF_ORG_REGEX = re.compile(r"(enterprise_aff(?=_organization))")


#Generate lists of event files that match the event of interest
event_dict = {'PERSON_MEMBER': PERSON_MEMBER_REGEX,
              'PERSON': PERSON_REGEX,
              'ADDRESS': ADDRESS_REGEX,
              'AFF_MEMBER': AFF_MEMBER_REGEX,
              'AFF_ORG': AFF_ORG_REGEX}

#Retrieve a list of bucket file paths mapped to the event of interest. 
def event_file_list(BucketPath: Literal['archive_bucket', 'production_bucket', 'destination_bucket'], 
                    BucketPrefix: Optional[str] = None, eventRegex=event_dict) -> any:
    
    d = {}
    for key, value in eventRegex.items():
      l = []
      for item in BucketPath.objects.filter(Prefix=BucketPrefix):
        regexMatch = re.search(value, item.key)

        if regexMatch:
           l.append(item.key)

      d[key] = l

    return d


'''
  To generate a list of file paths for each event of interest, run create_local_event_file_list below and
  you'll get a local copy.
'''

def create_local_event_file_list(fileName: str) -> None:
  events = json.dumps(event_file_list(archive_bucket, 'prod-initial-load'))


  with open(f"{fileName}.json", 'w') as file:
    file.write(events)

#create_local_event_file_list('initial_load_files_list')

#Open local copy of events list created and process for initial load or test file generation:
with open('initial_load_files_list.json') as e:
   event_paths = json.load(e)


#Let's generate a test file first:
update_dict = {}
insert_dict = {}
for key, values in event_paths.items():
  update_dict[key] = values[0]
  
  if key != 'AFF_ORG':
    insert_dict[key] = random.choice(values)
  

'''
  //download file into temp folder.
  //unzip it
  //open with pandas json lines = true

'''
fileLoc = 'prod-initial-load/part-0000/events/unionware/person_member_2023-09-11T22:59:31.150Z.jsonl.gz'

#Given a file, will return a sample of 10 events in a list
def processAndReturnSampleEvents(file):
  data = []
  with tempfile.NamedTemporaryFile() as f:
    archive_bucket.download_fileobj(file, f)
    
    with gzip.open(f.name, 'rb') as g:
      df = pd.read_json(g, lines=True)
      df2 = pd.DataFrame()
    
      df2.append(df.sample(10), ignore_index=True)

      data.append(df2)

    f.close()
  
  return data

    
#processAndReturnSampleEvents(fileLoc)

def returnSampleEventsByEvent(eventType, eventDict):
  d = {}

  for k,v in eventDict.items():
    if eventType == 'INSERT':
      d[k] = processAndReturnSampleEvents(v)

  print(d)

returnSampleEventsByEvent('INSERT', insert_dict)
   # 2. If a valid link, attempt to unzip and read it in with pandas json

   # 3. Check if file has dupe data...log file name and number of non-unique key values. Sort by ts, take latest value if dupes. 

   # 4. 





















        
#Connect to AWS S3 bucket with initial data partitions



#Loop through each partition and retrieve but don't store zip file (leave in memory)

#Put each files data in one temp file bc you'll need to check for duplicates across all partitions

#Sort by ts and keep latest duplicated value if existent.

#Load files from prod_file...follow same steps as above to get up to date, non-repeating data.

#Compare initial and prod files, update rows with prod if prod is not in initial or has update.

#Based on row numbers, cut data into chunks to allow for parallel load on s3 bucket update.

#Connect to s3 bucket and update each tables folder with initialized data.
