## Backing up and Restoring Data to Timestream for InfluxDB in Parts

After you have migrated to Timestream for InfluxDB, you may find a need to backup data to storage before restoring that data later to Timestream for InfluxDB, possibly with a Lambda function. The Influx CLI can be used for this purpose. The [`influx backup` command](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/backup/) can be used to back up InfluxDB data and the [`influx restore` command](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/restore/) can be used to restore that data.

### Backing up and Restoring Manually

Use the following steps to manually migrate a bucket from a destination instance to a source instance. Provide the following:

* `<source-host>`: The full URL of your source instance, for example, `https://influxdb-endpoint:8086`.
* `<source-token>`: An operator token from your source instance.
* `<source-org>`: The organization that your source bucket resides in.
* `<source-bucket-name>`: The name of the bucket in your source instance that you want to migrate.
* `<destination-host>`: The full URL of your destination instance.
* `<destination-token>`: An operator token from your destination instance.
* `<destination-org>`: The organization in your destination org where you want to put your migrated bucket.
* `<new-bucket-name>`: The name to give the newly migrated bucket in the destination instance. This bucket must not already exist.

#### Steps

1. [Install the Influx CLI](https://docs.influxdata.com/influxdb/v2/tools/influx-cli/).
1. Create a directory to hold your backup data:

   ```
   mkdir backup_directory
   ```

1. Backup your data from a bucket in your Timestream for InfluxDB instance:

   ```
   influx backup \
     --host <source-host> \
     --token <source-token> \
     --org <source-org> \
     --bucket <source-bucket-name> \
     backup_directory
   ```

1. Restore that data to a new bucket in the destination instance:

   ```
   influx restore \
     --host <destination-host> \
     --token <destination-token> \
     --org <destination-org> \
     --bucket <source-bucket-name> \
     --new-bucket <new-bucket-name> \
     backup_directory
   ```

### Backing up and Restoring with a Lambda Function

The following example shows how the Influx CLI can be packaged with Python code and deployed as a Lambda function to do periodic Timestream for InfluxDB backups and restores of a single bucket. This example is intended to serve as a starting point.

The Influx v2 API does not currently support backing up and restoring. Therefore, `influx backup` and `influx restore` must be run as subprocesses in a Lambda function. This requires that the Influx CLI be packaged with the Lambda function's code.

The file `bootstrap.py` contains Lambda function Python code that does periodic backups and restores, relying on the Influx CLI.

**Note**:

* If a bucket with the name `BUCKET_NAME` already exists in the instance that is being restored to, this bucket will be deleted before being recreated with new data. This is because `influx restore` cannot restore to an already existing bucket.
* Timezones must be considered. What the Lambda considers to be Friday or Monday may not align with your local timezone.
* Lambdas have a maximum runtime limit of 15 minutes. Take this into account when attempting to migrate large amounts of data. If a backup or restore cannot be completed in 15 minutes, consider replicating this example with an [Amazon EC2](https://aws.amazon.com/ec2/) instance instead.

#### Steps

1. Download and extract the Influx CLI for Linux ARM with the following command:

   ```
   wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-arm64.tar.gz && \
   tar -xzf influxdb2-client-2.7.5-linux-arm64.tar.gz
   ```

1. Use the following command to package the Influx CLI and Lambda function Python code together:

   ```
   zip -r package.zip bootstrap.py influx
   ```

1. If you haven't already, [download and install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
1. Create the execution role for the backup Lambda function. This role will allow the Lambda to read and write from an S3 bucket and be triggered by [Amazon EventBridge](https://aws.amazon.com/eventbridge/). Use the following command to create the role:

   ```
   aws iam create-role \
     --role-name ScheduledBackupAndRestoreLambdaExecutionRole \
     --assume-role-policy-document '{
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Principal": {
             "Service": "lambda.amazonaws.com"
           },
           "Action": "sts:AssumeRole"
         },
         {
           "Effect": "Allow",
           "Principal": {
             "Service": "events.amazonaws.com"
           },
           "Action": "sts:AssumeRole"
         }
       ]
     }'
   ```

1. Attach the `AWSLambdaBasicExecutionRole` to allow the Lambda function to write logs to CloudWatch:

   ```
   aws iam attach-role-policy \
     --role-name ScheduledBackupAndRestoreLambdaExecutionRole \
     --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
   ```

1. Create an S3 bucket that the Lambda function will use for backup data: reading from, writing to, and deleting objects in, replacing `<region>` with the AWS region you want your S3 bucket to be in:

   ```
   aws s3api create-bucket \
     --bucket <s3-bucket-name> \
     --region <region> \
     --create-bucket-configuration \
     LocationConstraint=<region>
   ```

1. Make sure public access to this S3 bucket is blocked:

   ```
   aws s3api put-public-access-block \
     --bucket <s3-bucket-name> \
     --public-access-block-configuration '{
       "BlockPublicAcls": true,
       "IgnorePublicAcls": true,
       "BlockPublicPolicy": true,
       "RestrictPublicBuckets": true
     }'
   ```

1. Attach a custom inline policy to the role to allow the Lambda function to read, write, and delete objects in an S3 bucket. Replace `<s3-bucket-name>` with the name of the S3 bucket you want to use:

   ```
   aws iam put-role-policy \
     --role-name ScheduledBackupAndRestoreLambdaExecutionRole \
     --policy-name S3ReadAndWriteAccessPolicy \
     --policy-document '{
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Action": [
             "s3:GetObject",
             "s3:PutObject",
             "s3:ListBucket",
             "s3:DeleteObject"
           ],
           "Resource": [
             "arn:aws:s3:::<s3-bucket-name>",
             "arn:aws:s3:::<s3-bucket-name>/*"
           ]
         }
       ]
     }'
   ```

1. Get the ARN of the Lambda's execution role:

   ```
   aws iam get-role \
     --role-name ScheduledBackupAndRestoreLambdaExecutionRole \
     --query "Role.Arn" \
     --output text
   ```

1. Make sure that the following environment variables are set:

    * `BACKUP_ENDPOINT`: The full URL of the Timestream for InfluxDB instance you want to backup from, for example, `https://influxdb-endpoint:8086`.
    * `BACKUP_TOKEN`: An operator token from the Timestream for InfluxDB instance you want to backup from.
    * `BACKUP_ORG`: The name of the organization that the bucket resides in.
    * `RESTORE_ENDPOINT`: The full URL of the Timestream for InfluxDB instance you want to restore to.
    * `RESTORE_TOKEN`: An operator token from the Timestream for InfluxDB instance you want to restore to.
    * `RESTORE_ORG`: The name of the organization that the restored bucket will reside in.
    * `BUCKET_NAME`: The name of the Timestream for InfluxDB bucket you want to backup from and restore to.
    * `S3_BUCKET_NAME`: The name of the S3 bucket you want to store backup data in. When backups are performed, this bucket will be updated with backup files. When restores are performed, this bucket will be read from.

1. Deploy the Lambda function using the AWS CLI. It will use Timestream for InfluxDB tokens and endpoints to make back ups and restore data. Use the following command to deploy the Lambda function, replacing `<execution-role-arn>` with the output from step 9 and `<region>` with the AWS region you want to deploy the function in:

   ```
   aws lambda create-function \
     --role <execution-role-arn> \
     --function-name backup-and-restore-lambda \
     --architectures 'arm64' \
     --region <region> \
     --runtime python3.13 \
     --timeout 900 \
     --handler bootstrap.lambda_handler \
     --zip-file fileb://package.zip \
     --environment Variables="{
       BACKUP_ENDPOINT=${BACKUP_ENDPOINT},
       BACKUP_TOKEN=${BACKUP_TOKEN},
       BACKUP_ORG=${BACKUP_RG},
       RESTORE_ENDPOINT=${RESTORE_ENDPOINT},
       RESTORE_TOKEN=${RESTORE_TOKEN},
       RESTORE_ORG=${RESTORE_ORG},
       BUCKET_NAME=${BUCKET_NAME},
       S3_BUCKET_NAME=${S3_BUCKET_NAME}
     }"
   ```

1. Create a rule in Amazon EventBridge to call the Lambda function every Friday and Monday, replacing `<region>` with the AWS region you want to create the rule in:

   ```
   aws events put-rule \
     --name RunLambdaOnFridayAndMonday \
     --region <region> \
     --schedule-expression "cron(0 12 ? * 2,6 *)" \
     --state ENABLED
   ```

1. Add a permission to the Lambda function to allow it to be run using this rule, replacing `<region>` with the AWS region you deployed the Lambda function in and `<account-id>` with your AWS account ID:

   ```
   aws lambda add-permission \
     --function-name backup-and-restore-lambda \
     --region <region> \
     --statement-id EventBridgeInvokePermission \
     --action lambda:InvokeFunction \
     --principal events.amazonaws.com \
     --source-arn arn:aws:events:<region>:<account-id>:rule/RunLambdaOnFridayAndMonday
   ```

1. Get the Lambda function's ARN, replacing `<region>` with the AWS region your Lambda function is in:

   ```
   aws lambda get-function \
     --function-name backup-and-restore-lambda \
     --region <region> \
     --query "Configuration.FunctionArn" --output text
   ```

1. Attach the Lambda function as a target to the EventBridge rule, replacing `<lambda-function-arn>` with the output from step 14 and `<region>` with the AWS region your rule is in:

   ```
   aws events put-targets \
     --region <region>
     --rule RunLambdaOnFridayAndMonday \
     --targets "Id"="1","Arn"="<lambda-function-arn>"
   ```

1. Verify the rule and target, replacing `<region>` with the AWS region you created the rule in:

   ```
   aws events list-rules --region <region> --name-prefix RunLambda &&
   aws events list-targets-by-rule --region <region> --rule RunLambdaOnFridayAndMonday
   ```

