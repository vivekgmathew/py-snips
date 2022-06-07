import paramiko
import boto3
import json
import os

# Real value


def lambda_handler(event, context):
    env = os.getenv('env')
    region = os.getenv('region')

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    key = f"dm/snp/{env}"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )

    secrets = json.loads(get_secret_value_response['SecretString'])
    ssh_client.connect(hostname=secrets["server"], username=secrets["user"], password=secrets["pass"])

    ftp = ssh_client.open_sftp()
    new_fl_name = ""

    fl = ftp.open("/home/ec2-user/file.txt", bufsize=32768).read()
    s3 = boto3.resource('s3')
    result = s3.meta.client.put_object(Body=fl, Bucket=f"dm-sp-pr-{env}", Key=f"data/{fl}")
    print("Completed ...")


