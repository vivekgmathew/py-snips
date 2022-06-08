import paramiko
import boto3
import json
import datetime

from pytz import timezone
from boto3.dynamodb.conditions import Key

def get_next_time(file_name):
    time = file_name[14:19]
    hour = time.split("-")[0]
    minute = time.split("-")[1]
    if minute == "00":
        minute = "15"
    elif minute == "15":
        minute = "30"
    elif minute == "30":
        minute = "45"
    elif minute == "45":
        minute = "00"
        hour = str(int(hour) + 1)

    new_time = f"{hour}-{minute}"
    return new_time

FILE_PATH = "/home/ec2-user/"

tz = timezone('EST')
cd = datetime.datetime.now(tz)
formatted_date = datetime.date.strftime(cd, "%m-%d-%Y")

ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

secret_name = "dm/snp/dev"
region_name = "us-east-2"

# Create a Secrets Manager client
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)
get_secret_value_response = client.get_secret_value(
    SecretId=secret_name
)

dyn_client = boto3.resource(
    service_name='dynamodb',
    region_name=region_name
)

tbl_file_tracker = dyn_client.Table("dm-file-tracker-dev")

r_file = f"CMQ{formatted_date}"
a_file = f"CMQ{formatted_date}"
e_file = f"CMQ{formatted_date}"
items = None

try:
    items = tbl_file_tracker.query(
        KeyConditionExpression=Key('source_type').eq('S&P'), IndexName='source_type-index'
    )['Items']

    for item in items:
        if r_file not in item['last_file']:
            tbl_file_tracker.delete_item(
                Key={
                    'file_type': 'R'
                }
            )
        if a_file not in item['last_file']:
            tbl_file_tracker.delete_item(
                Key={
                    'file_type': 'A'
                }

            )
        if e_file not in item['last_file']:
            tbl_file_tracker.delete_item(
                Key={
                    'file_type': 'E'
                }
            )
    # Query again
    items = tbl_file_tracker.query(
        KeyConditionExpression=Key('source_type').eq('S&P'), IndexName='source_type-index'
    )['Items']
    print("New query item length " + str(len(items)))
    for item in items:
        if item['file_type'] == 'A':
            next_time = get_next_time(item['last_file'])
            a_file = f"{a_file}-{next_time}A.PIP"
        if item['file_type'] == 'E':
            next_time = get_next_time(item['last_file'])
            e_file = f"{e_file}-{next_time}E.PIP"
        if item['file_type'] == 'R':
            next_time = get_next_time(item['last_file'])
            r_file = f"{r_file}-{next_time}R.PIP"

except Exception as ex:
    print("Empty tracker table ..." + ex)

if len(items) < 1:
    r_file = r_file + "-09-00R.PIP"
    a_file = a_file + "-09-00A.PIP"
    e_file = e_file + "-09-00E.PIP"


secrets = json.loads(get_secret_value_response['SecretString'])
ssh_client.connect(hostname=secrets["server"], username=secrets["user"], password=secrets["pass"])

ftp = ssh_client.open_sftp()

files = [r_file, a_file, e_file]
for file in files:
    try:
        full_file = f"{FILE_PATH}{file}"
        fl = ftp.open(full_file, bufsize=32768).read()
        print(fl)
        s3 = boto3.resource('s3')
        result = s3.meta.client.put_object(Body=fl, Bucket='dm-sp-pr', Key=f'data/{file}')
        file_type = file.split(".")[0][-1:]

        print("Reached here")
        # Delete the entry for the file type
        tbl_file_tracker.delete_item(
            Key={
                'file_type': file_type
            }
        )

        # Add new entry with file name
        tbl_file_tracker.put_item(
            Item={
                'file_type': file_type,
                'source_type': 'S&P',
                'last_file': file
            }
        )
    # Add dynamo db write
    except IOError as ex:
        print("File Not Found" + full_file)






