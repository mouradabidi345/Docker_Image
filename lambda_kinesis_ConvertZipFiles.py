import requests
import pandas as pd
import gzip
import json
import datetime
import boto3
from pandas import json_normalize


aws_access_key_id = ""
aws_secret_access_key = ""

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name="us-east-2"
)

kinesis_client = session.client('firehose')

def sendToStream(KinesisData):
    response = kinesis_client.put_record(
        DeliveryStreamName = 'Test_DataSource_stream',
        Record = {
            'Data': str(KinesisData)
        }
    )
    return response



data = []
def ConvertZip(path):
    with gzip.open(path , 'rb') as gzip_file:
        for line in gzip_file:  # Read one line.
            line = line.rstrip()
            line = line.decode("utf-8")
            line = json.loads(line)
            #Convert json to a dataframe and create a primary key called natural_key so that we uniquify every row
            df = json_normalize(line)
            df['natural_key'] =  str(df['reviewerName']) + str(df['unixReviewTime'])
            df['natural_key'] =  df['reviewerName'].map(str) + df['unixReviewTime'].map(str)
            df['natural_key']= df['natural_key'].str.replace(' ', '')
            #convert the dataframe to json
            result = df.to_json(orient="records", date_format='iso')
            parsed = json.loads(result)
            result1 =json.dumps(parsed, indent=4)
            data.append(parsed)
            break
            
    return data
# result = ConvertZip()

def lambda_handler(request):
    

    if 'cursor' in request['state']:
        time = request['state']['cursor']
        print(time)
    else:
        time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        print(time)
    
    path = request['secrets']['path']
    result = ConvertZip(path)
    
    #the table that will have the data will be called RawReviewData, the name of its primary key will be natural_key
    KinesisData = {
        "schema": {
            "RawReviewData": {
                "primary_key": [
                    "natural_key"
                ]
            }
        },
        "state": {
            "cursor": time
        },
        "RawReviewData" :result
    }
    # KinesisData['RawReviewData'] = result


    response = sendToStream(KinesisData) ## this response will be send  to Kinesis (line 29 )
    headers = {"Content-Type": "application/json"}
    # I send the response to a file in my computer for testing:
    with open('Z:\Shared\Public\BI_Test\Output1.txt', 'w') as outfile:
            json.dump(KinesisData, outfile, sort_keys = True, indent = 4,
            ensure_ascii = False)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "Region ": KinesisData
        })
    }
request = {
    'state': {},
    'secrets': {
        'path' : 'C:\complete.json.gz'
        
    }

}
out = lambda_handler(request)      


