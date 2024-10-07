# InfluxDB Timestream Connector

## Overview

The InfluxDB Timestream connector allows [line protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/) to be ingested to [Amazon Timestream for LiveAnalytics](https://aws.amazon.com/timestream/). The connector parses ingested line protocol and maps the data to multi-measure records for ingestion into Timestream for LiveAnalytics using the [Timestream Write API](https://docs.aws.amazon.com/timestream/latest/developerguide/API_Operations_Amazon_Timestream_Write.html).

### Architecture

The following diagram shows a high-level overview of the connector's architecture when deployed as an [AWS Lambda](https://aws.amazon.com/lambda/) function.

<img src="./docs/img/influxdb_timestream_connector_lambda_function_arch.png" width=700/>

## Table Schema

### Multi-Table Multi-Measure

The following table shows how the connector maps line protocol elements to Timestream for LiveAnalytics record attributes.

| Line Protocol Element | Timestream Record Attribute |
|-----------------------|---------------------------|
| Timestamp             | Time                      |
| Tags                  | Dimensions                |
| Fields                | Measures                  |
| Measurements          | Table names               |

A Timestream record's `measure_name` field is not derived from any element of ingested line protocol. Due to the multi-measure record translation, the connector sets the `measure_name` for each multi-measure record to the value of a Lambda environment variable. When [deployed as part of a CloudFormation stack](#aws-cloudformation-deployment), this can be customized by overriding the `MeasureNameForMultiMeasureRecords` parameter. When [deployed locally](#local-deployment), this can be customized by setting the `measure_name_for_multi_measure_records` environment variable.

The following example shows the translation of a single line protocol point into a Timestream for LiveAnalytics table, using a Timestamp with second precision and a Lambda environment variable configured to `influxdb-measure`:

#### Line Protocol Point

```
cpu_load_short,host=server01,region=us-west value=0.64,average=1.24, 1725059274
```

#### Resulting cpu_load_short Timestream for LiveAnalytics Table

| host     | region  | measure_name     | time                          | value | average |
|----------|---------|------------------|-------------------------------|-------|---------|
| server01 | us-west | influxdb-measure | 2024-08-30 23:07:54.000000000 | 0.64  | 1.24    |

## Deployment Options

### AWS CloudFormation Deployment

The InfluxDB Timestream connector can be deployed within an AWS CloudFormation stack as an AWS Lambda function with an accompanying Amazon REST API Gateway. The API Gateway mimics the InfluxDB v2 API and provides the `/api/v2/write` endpoint for ingestion.

#### Deploying a CloudFormation Stack Using SAM CLI

The stack can be deployed using the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/using-sam-cli.html), [Cargo Lambda](https://www.cargo-lambda.info/) to package the code, and `template.yml`.

##### Stack Parameters

The following parameters are available when deploying the connector as part of a CloudFormation stack. An example of setting these parameters is included in step 4 of the [SAM deployment steps](#sam-deployment-steps).

| Parameter     | Description | Default Value |
|---------------|-------------|---------------|
| `DatabaseName`  | The name of the database to use for ingestion. | `influxdb-line-protocol` |
| `LambdaMemorySize` | The size of the memory in MB allocated per invocation of the function. | `128` |
| `LambdaName` | The name to use for the Lambda function. | `influxdb-timestream-connector-lambda` |
| `LambdaTimeoutInSeconds` | The number of seconds to run the Lambda function before timing out. | `30` |
| `MeasureNameForMultiMeasureRecords` | The value to use in records as the `measure_name`, as shown in the [example line protocol to Timestream records translation](#resulting-cpu_load_short-timestream-for-liveanalytics-table). | `influxdb-measure` |
| `RestApiGatewayName` | The name to use for the REST API Gateway. | `InfluxDB-Timestream-Connector-REST-API-Gateway` |
| `RestApiGatewayStageName` | The name to use for the REST API Gateway stage. | `dev` |
| `RestApiGatewayTimeoutInMillis` | The maximum number of milliseconds a REST API Gateway event will wait before timing out. | `30000` |
| `WriteThrottlingBurstLimit` | The number of burst requests per second that the REST API Gateway permits. | `1200` |

##### SAM Deployment Steps

1. [Download and install the AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html).
2. [Download and install Rust](https://www.rust-lang.org/tools/install).
3. [Download and install Cargo Lambda](https://www.cargo-lambda.info/guide/installation.html).
4. Run the following command to package the binary in `target/lambda/influxdb-timestream-connector/bootstrap.zip`, where `template.yml` expects it to be, and cross compile for Linux ARM:
    ```
    cargo lambda build --release --arm64 --output-format zip
    ```
5. Run the following command, replacing `<region>` with the AWS region you want to deploy in and providing parameter overrides as desired. Note that this example command uses the provided `samconfig.toml` file and by default sets the name of the stack to `InfluxDBTimestreamConnector`.

    ```shell
    sam deploy template.yml \
        --region <region> \
        --parameter-overrides \
            ParameterKey1=ParameterValue1 \
            ParameterKey2=ParameterValue2
    ```
6. Once the stack has finished deploying, take note of the output `Endpoint` value. This value will be used as the endpoint for all write requests and is analogous to an [InfluxDB host address](https://docs.influxdata.com/influxdb/v2/reference/urls/) and is used in the same way, for example, `<endpoint>/api/v2/write`.

#### Lambda Dead Letter Queue

If the connector was deployed with async invocation, then all client requests will be returned a response with a `202` status code, indicating that the request has been received and is being processed. If the request fails, the client will not be notified. Instead, failed requests will either be [logged by the REST API Gateway](#viewing-rest-api-gateway-logs) or be added to the Lambda's dead letter queue, where the failed request can be reviewed in full. The name of the dead letter queue is provided as the `LambdaDeadLetterQueueName` output when deploying the stack.

To access the Lambda's dead letter queue and view any possible stored messages:

1. Take note of the dead letter queue's name as provided by the `LambdaDeadLetterQueueName` output upon successful stack deployment.
2. Visit the [Amazon SQS console](https://console.aws.amazon.com/sqs/v3/home).
3. In the navigation pane, choose **Queues**.
4. Find and select the SQS queue with the same name as indicated by `LambdaDeadLetterQueueName`.
5. Choose **Send and receive messages**.
6. Choose **Poll for messages**.

#### Stack Logs

##### Viewing REST API Gateway Logs

By default, access logging is enabled for the REST API Gateway.

To view logs:

1. Visit the [AWS CloudFormation console](https://console.aws.amazon.com/cloudformation/home).
2. In the navigation pane, choose **Stacks**.
3. Choose your deployed stack from the list of stacks.
4. In the **Resources** tab choose the **Physical ID** of the `RestApiGatewayLogGroup` resource.

##### Viewing Lambda Logs

By default, logging is enabled for the deployed connector.

To view logs:

1. Visit the [AWS CloudFormation console](https://console.aws.amazon.com/cloudformation/home).
2. In the navigation pane, choose **Stacks**.
3. Choose your deployed stack from the list of stacks.
4. In the **Resources** tab choose the **Physical ID** of the Lambda function.
5. In the **Monitor** tab, choose **View CloudWatch logs**.

### Local Deployment

The connector can be run locally using [Cargo Lambda](https://www.cargo-lambda.info/guide/what-is-cargo-lambda.html).

1. [Download and install Rust](https://www.rust-lang.org/tools/install).
2. [Configure your AWS credentials for use by the AWS SDK for Rust](https://docs.aws.amazon.com/sdkref/latest/guide/creds-config-files.html).
3. [Download and install Cargo Lambda](https://www.cargo-lambda.info/guide/installation.html).
4. Configure the following environment variables:
    - `region` string: the AWS region to use. Defaults to `us-east-1`.
    - `database_name` string: the Timestream for LiveAnalytics database name to use. Defaults to `influxdb-line-protocol`.
    - `measure_name_for_multi_measure_records` string: the value to use in records as the measure name. Defaults to `influxdb-measure`.
5. To run the connector on `http://localhost:9000` execute the following command:

    ```shell
    cargo lambda watch
    ```
6. Send all requests to `http://localhost:9000/api/v2/write`.

## Security

### Encryption

When the connector is deployed as part of a CloudFormation stack, the stack's API Gateway ensures all communication is protected by TLS 1.2+.

### Authentication

The REST API Gateway ensures all requests are authenticated with SigV4. Any InfluxDB user tokens or credentials are discarded. This authentication method cannot be correlated to IAM authorization.

## IAM Permissions

### IAM Deployment Permissions

[//]: # (TODO: Update deployment policy with least privilege once REST API Gateway deployment has been tested.)

The following is the least privileged IAM permissions for deploying the connector.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "iam:CreateRole",
                "iam:AttachRolePolicy",
                "iam:UpdateAssumeRolePolicy",
                "iam:PassRole",
                "iam:PutRolePolicy"
            ],
            "Resource": [
                "arn:aws:iam::{account-id}:role/AWSLambdaBasicExecutionRole",
                "arn:aws:iam::{account-id}:role/cargo-lambda-role*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lambda:CreateFunction",
                "lambda:UpdateFunctionCode",
                "lambda:GetFunction",
                "lambda:UpdateFunctionConfiguration",
                "lambda:GetFunctionConfiguration",
                "lambda:CreateFunctionUrlConfig"
            ],
            "Resource": "arn:aws:lambda:{region}:{account-id}:function:influxdb-timestream-connector"
        }
    ]
}
```

### IAM Execution Permissions

The following is the least privileged IAM permissions for executing the connector.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "timestream:WriteRecords",
                "timestream:Select",
                "timestream:DescribeTable",
				"timestream:CreateTable"
            ],
            "Resource": "arn:aws:timestream:{region}:{account-id}:database/influxdb-line-protocol/table/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "timestream:DescribeEndpoints"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "timestream:DescribeDatabase",
				"timestream:CreateDatabase"
            ],
            "Resource": "arn:aws:timestream:{region}:{account-id}:database/influxdb-line-protocol"
        }
    ]
}
```

## Example Application with Go Client

A demo Go client application is available under `sample-code/aws-timestream/sample-influxdb-clients/go/line-protocol-client-demo.go` that sends line protocol data to a local or deployed instance of the connector. A line protocol sample dataset is included in `sample-code/aws-timestream/sample-influxdb-clients/data/bird-migration.line`, which the demo application uses for ingestion.

To configure the sample application and ingest all line protocol data contained in `bird-migration.line` to Timestream for LiveAnalytics, perform the following steps:

1. [Download and install Go](https://go.dev/doc/install).
2. [Configure your AWS credentials for use by the AWS SDK for Rust](https://docs.aws.amazon.com/sdkref/latest/guide/creds-config-files.html).
3. [Deploy the Timestream Line Protocol connector locally](#local-deployment).
4. Navigate to `sample-code/aws-timestream/sample-influxdb-clients/go/`.
5. Run the sample Go client:
    - With the connector deployed in a CloudFormation stack, replacing `<region>` with the AWS region you deployed your stack in and `<endpoint>` with the endpoint of your deployed REST API Gateway:
        ```shell
        go run line-protocol-client-demo.go \
            --region <region> \
            --service execute-api \
            --endpoint <endpoint>
        ```
    - With the connector deployed locally:
        ```shell
        go run line-protocol-client-demo.go
        ```
6. Run the following [AWS CLI](https://aws.amazon.com/cli/) command to verify data has been ingested to Timestream for LiveAnalytics, replacing `<region>` with the region you used for ingesting data:
    ```shell
    aws timestream-query query \
        --region <region> \
        --query-string 'SELECT * FROM "influxdb-line-protocol"."migration1" LIMIT 10'
    ```

## Troubleshooting

### Amazon Timestream Write API Errors

| Error | Status Code | Description | Solution |
|-------|-------------|-------------|----------|
| `AccessDeniedException` | 400 | You are not authorized to perform this action. | Verify that your user has the [correct permissions](#iam-permissions). |
| `InternalServerException` | 500 | Timestream was unable to fully process this request because of an internal server error. | Try ingesting records at another time. |
| `RejectedRecordsException` | 400 | The records sent to Timestream were invalid. | Check the [line protocol limitations](#line-protocol-limitations) and [caveats](#caveats) and ensure your line protocol points abide by expected formatting.
| `ResourceNotFoundException` | 400 | The operation tried to access a nonexistent resource. The resource might not be specified correctly, or its status might not be ACTIVE. | Verify that your database exists in Timestream for LiveAnalytics. Consider setting `enable_database_creation` to `true` to allow the connector to create the database for you. |
| `ServiceQuotaExceededException` | 400 | The instance quota of resource exceeded for this account. | Confirm you are not exceeding Timestream for LiveAnalytics' quotas. If you are not exceeding any quotas, check the list of [line protocol limitations](#line-protocol-limitations) below, specifically the number of unique measurement names. |
| `ThrottlingException` | 400 | Too many requests were made by a user and they exceeded the service quotas. The request was throttled. | Decrease the rate of your requests. |

## Testing

### Requirements

1. [Configure your AWS credentials for use by the AWS SDK for Rust](https://docs.aws.amazon.com/sdkref/latest/guide/creds-config-files.html).
2. Ensure your IAM permissions include the permissions listed in the [IAM Execution Permissions](#iam-execution-permissions) section.

### All Tests

To run all tests, including integration tests and unit tests, use the following command:

```shell
cargo test -- --test-threads=1
```

### Integration Tests

To run all integration tests, use the following command, from the project root:

```shell
cargo test --test '*' -- --test-threads=1
```

> **NOTE**: It is important to use the flag `--test-threads=1` in order to avoid throttling errors, as the integration tests will create and delete tables.

To run a specific integration test, use the following command:

```shell
cargo test <integration test name>
```

### Unit Tests

To run all unit tests, use the following command:

```shell
cargo test --lib
```

To run a single unit test, use the following command:

```shell
cargo test <unit test name>
```

## Limitations

### Line Protocol Limitations

Due to the connector translating line protocol to Timestream records, line protocol must satisfy [Timestream for LiveAnalytics' quotas](https://docs.aws.amazon.com/timestream/latest/developerguide/ts-limits.html).

| Line Protocol Component | Limitation |
|-------------------------|------------|
| Maximum size of a line protocol point, with `measure_name` included. | 2 Kilobytes |
| Number of unique tag keys per table. | 128 |
| Maximum tag key size. | 60 bytes |
| Maximum measurement name size. | 256 bytes |
| Maximum unique measurement names per database (using multi-table multi-measure schema). | 50,000 |
| Maximum field key size. | 256 bytes |
| Maximum number of fields per point. | 256 |
| Maximum field value size. | 2048 bytes |
| Maximum unique field key values. | 1024 |
| Latest valid timestamp. | Fifteen minutes in the future from the current time. |
| Oldest valid timestamp. | `mag_store_retention_period` days before the current time. |

### Database and Table Creation Delay

There is a delay of one second added before deleting or creating a table or database. This is because of Timestream for LiveAnalytics' "Throttle rate for CRUD APIs" [quota](https://docs.aws.amazon.com/timestream/latest/developerguide/ts-limits.html#limits.default) of one table/database deletion/creation per second.

## Caveats

### Line Protocol Tag Requirement

In order to ingest to Timestream for LiveAnalytics, every line protocol point must include at least one tag.

### Query String Parameters

The connector expects query string parameters to be included as `queryParameters` or `queryStringParameters` in requests.

