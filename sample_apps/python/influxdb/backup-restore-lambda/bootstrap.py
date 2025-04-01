import boto3
from botocore.exceptions import ClientError
import datetime
import os
import shutil
import subprocess

def lambda_handler(event, context):
    session = boto3.session.Session()

    backup_token = os.environ['BACKUP_TOKEN']
    backup_endpoint = os.environ['BACKUP_ENDPOINT']
    backup_org = os.environ['BACKUP_ORG']

    restore_token = os.environ['RESTORE_TOKEN']
    restore_endpoint = os.environ['RESTORE_ENDPOINT']
    restore_org = os.environ['RESTORE_ORG']

    bucket_name = os.environ['BUCKET_NAME']

    s3_bucket_name = os.environ['S3_BUCKET_NAME']

    day_of_the_week = datetime.datetime.now().strftime('%A')

    if day_of_the_week == 'Friday':
        result = backup(session, backup_endpoint, backup_token, s3_bucket_name, bucket_name, backup_org)
    elif day_of_the_week == 'Monday':
        result = restore(session, restore_endpoint, restore_token, s3_bucket_name, bucket_name, restore_org)

    return {
        "statusCode": 200,
        "body": result
    }

def backup(session: boto3.session, backup_endpoint: str, backup_token: str, s3_bucket_name: str,
           bucket_name: str, org_name: str) -> str:
    s3_client = session.client('s3')
    s3_bucket = session.resource('s3').Bucket(s3_bucket_name)

    backup_path = "/tmp/backup_directory"

    # Delete backup directory if it already exists and recreate it.
    if os.path.exists(backup_path):
        shutil.rmtree(backup_path)
    os.makedirs(backup_path)

    # Backup data to a directory.
    bucket_backup_command = ['./influx', 'backup', backup_path, '--token', backup_token,
                             '--host', backup_endpoint, '--bucket', bucket_name,
                             '--org', org_name]
    try:
        subprocess.run(bucket_backup_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
    except subprocess.CalledProcessError:
        raise RuntimeError("Backup failed")
    
    # Delete the contents of the S3 bucket if there are any.
    s3_bucket.objects.all().delete()

    # Put backup data in the S3 bucket, to be used when restoring later.
    for root, _, files in os.walk(backup_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = os.path.relpath(local_file_path, backup_path)
            try:
                s3_client.upload_file(local_file_path, s3_bucket_name, s3_key)
            except ClientError as e:
                print(e)
                raise
    
    return f"Finished backing up data to {s3_bucket_name} S3 bucket"
    
def restore(session: boto3.session, restore_endpoint: str, restore_token: str,
            s3_bucket_name: str, bucket_name: str, org_name: str) -> str:
    s3_bucket = session.resource('s3').Bucket(s3_bucket_name)

    restore_path = "/tmp/restore_directory"

    # Delete restore directory if it already exists and recreate it.
    if os.path.exists(restore_path):
        shutil.rmtree(restore_path)
    os.makedirs(restore_path)

    # Read stored backup data to a directory.
    for s3_object in s3_bucket.objects.all():
        local_file_path = os.path.join(restore_path, s3_object.key)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        s3_bucket.download_file(s3_object.key, local_file_path)
    
    # Delete the bucket, if it already exists.
    delete_bucket_command = ['./influx', 'bucket', 'delete', '--token',
                             restore_token, '--host', restore_endpoint,
                             '--org', org_name, '--name', bucket_name]
    subprocess.run(delete_bucket_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=False)
    
    # Restore data and to a new bucket.
    restore_command = ['./influx', 'restore', '--token',
                       restore_token, '--host', restore_endpoint, '--org',
                       org_name, "--bucket", bucket_name, "--new-bucket",
                       bucket_name, restore_path]
    try:
        subprocess.run(restore_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
    except subprocess.CalledProcessError:
        raise RuntimeError("Restore failed")
    
    return f"Finished restoring to {restore_endpoint}"
