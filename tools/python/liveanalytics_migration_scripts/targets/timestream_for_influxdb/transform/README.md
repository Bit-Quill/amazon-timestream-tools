# Timestream for LiveAnalytics to Line Protocol Translation Script

## Overview

The script in this directory converts [Amazon Timestream](https://aws.amazon.com/timestream/) for LiveAnalytics data into [line protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/), allowing ingestion to any database that supports line protocol.

Specifically, the script does the following:
- Loads exported Timestream for LiveAnalytics [data](https://docs.aws.amazon.com/timestream/latest/developerguide/API_Record.html) from an [Amazon S3](https://aws.amazon.com/s3/) bucket into an [Amazon Athena](https://aws.amazon.com/athena/) table.
- Translates the data stored in the Athena table into line protocol and stores it in the S3 bucket.

This script assumes that the path `<Timestream database name>/<Timestream table name>/unload-<%Y-%m-%d-%H:%M:%S>/results` exists in your S3 bucket and contains data unloaded by the [unload script](../../../unload/README.md). Line protocol data will be exported to `<Timestream database name>/<Timestream table name>/unload-<%Y-%m-%d-%H:%M:%S>/line-protocol-output` in your S3 bucket.

## Data Mapping

The following table shows how Timestream for LiveAnalytics data is mapped to line protocol data.

| Timestream for LiveAnalytics Concept | Line Protocol Concept |
|--------------------------------------|-----------------------|
| [Table](https://docs.aws.amazon.com/timestream/latest/developerguide/API_Table.html)                                | [Measurement](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#measurement)           |
| [Dimensions](https://docs.aws.amazon.com/timestream/latest/developerguide/API_Dimension.html)                           | [Tags](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#tag-set)                  |
| [Measure name](https://docs.aws.amazon.com/timestream/latest/developerguide/data-modeling.html#data-modeling-measurenamemulti)                         | Tag                   |
| [Measures](https://docs.aws.amazon.com/timestream/latest/developerguide/API_MeasureValue.html)                             | [Fields](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#field-set)                |
| [Time](https://docs.aws.amazon.com/timestream/latest/developerguide/writes.html#writes.data-types)                                 | [Timestamp](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#timestamp)             |

**NOTE**: The finest timestamp precision that Athena supports is **milliseconds**. If you need greater timestamp precision, such as microsecond or nanosecond precision, consider migrating to [Amazon RDS](https://aws.amazon.com/rds/).

## Prerequisites

The following prerequisites must be met before using the script:

1. [AWS credentials configured for use with boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file).
2. A Timestream for LiveAnalytics table [created](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.table.using-console).
3. Data from the Timestream for LiveAnalytics table having been unloaded to an S3 bucket, within the path `<S3 bucket name>/<Timestream database name>/<Timestream table name>/results`.
4. [Python 3.13 installed](https://www.python.org/downloads/).

## Installation

Optionally, a [Python virtual environment](https://docs.python.org/3/library/venv.html), with all packages in `../../../requirements.txt` installed. The following command can be used to create a virtual environment, activate it, and install all necessary packages:
    ```shell
    python3 -m venv env && \
    source env/bin/activate && \
    python3 -m pip install -r requirements.txt
    ```

## Usage

### Options

`transform.py` provides the following options:

- `-h`, `--help`: Show this help message and exit.
- `--tables TABLES`: Optional. A comma-separated list of Timestream for LiveAnalytics tables to translate.
- `--database-name DATABASE_NAME`: The Timestream for LiveAnalytics database that your table(s) resides in.
- `--all-tables`: Optional. Whether to translate all tables in the database.
- `--s3-bucket-name S3_BUCKET_NAME`: The name of the S3 bucket to load and unload data from. This bucket must already exist.
- `--athena-database-name ATHENA_DATABASE_NAME`: Optional. The name of the Athena database to use when creating any new Athena tables. Defaults to "`default`".
- `--athena-table-name ATHENA_TABLE_NAME`: Optional. The name to use for a new Athena table, used for the translation of LiveAnalytics records to line protocol. Defaults to the Timestream for LiveAnalytics database and table name connected with an underscore, without dashes.
- `--dimensions-to-fields DIMENSIONS_TO_FIELDS`: Optional. The tables and names of dimensions within to change to fields in resulting line protocol. Dimensions are usually mapped to tags. Mapping dimensions to fields can lower cardinality. The required format is `--dimensions-to-fields table1=dimension1,dimension2 --dimensions-to-fields table2=dimension3,dimension4`.
- `--add-validation-field`: Optional. Whether to add an additional field to all translated line protocol points to help with post-migration validation. The field will be `la_unload=1`.

### Basic Usage

To translate data stored in the bucket, `example_s3_bucket` from the Timestream for LiveAnalytics table `example_table` in `example_database`, run the following command:
```shell
python3 main.py \
    --database-name example_database \
    --tables example_table \
    --s3-bucket-name example_s3_bucket
```

After the script has finished running:
- In Athena, the table `example_database_example_table` will be created, containing Timestream for LiveAnalytics data.
- In Athena, the table `lp_example_database_example_table` will be created, containing Timestream for LiveAnalytics data translated to line protocol points.
- In the S3 bucket `example_s3_bucket`, within the path `example_database/example_table/unload-<%Y-%m-%d-%H:%M:%S>/line-protocol-output`, line protocol data will be stored.

### Multiple Tables

The `--tables` argument accepts any number of table names, where each named table belongs to the same database:

```shell
python3 main.py \
    --database-name example_database \
    --tables example_table_1,example_table_2,example_table_3 \
    --s3-bucket-name example_s3_bucket
```

### Using Dimensions as Fields

In Timestream for InfluxDB, [cardinality](https://docs.influxdata.com/influxdb/v2/reference/glossary/#series-cardinality) is the "number of unique measurement, tag set, and field key combinations in an InfluxDB bucket". By default, the script maps dimensions to tags. To reduce cardinality, dimensions can instead be mapped to fields. This should only be done if a dimension is not expected to be queried often, as fields are [not indexed](https://docs.influxdata.com/influxdb/v1/concepts/glossary/#field-value).

Dimensions belonging to a specific table can be changed to fields in the following way:
```shell
python3 main.py \
    --database-name example_database \
    --tables example_table_1,example_table_2,example_table_3 \
    --s3-bucket-name example_s3_bucket \
    --dimensions-to-fields example_table1=dimension_1,dimension_2 \
    --dimensions-to-fields example_table2=dimension_3,dimension_4 \
```

### Adding a Field for Validation

To help validate that all data from a Timestream for LiveAnalytics table has been migrated to Timestream for InfluxDB, an additional field can be added to all line protocol points. Adding an additional field ensures each data point has a unique identifier, preventing InfluxDB's deduplication logic from merging or omitting migrated records during validation. This field is `la_unload=1`.

To verify data in Timestream for InfluxDB, the following Flux query can be used, replacing `<Timestream table name>` with the name of your Timestream for LiveAnalytics table:
```
from(bucket: "example_influxdb_bucket")
    |> range(start: 0)
    |> filter(fn: (r) => r._measurement == "<Timestream table name>")
    |> filter(fn: (r) => r._field == "la_unload")
    |> group()
    |> count()
```

Flux queries can be executed using the [Influx CLI](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/) or in the [InfluxDB UI](https://docs.influxdata.com/influxdb/v2/query-data/execute-queries/data-explorer/).

The `la_unload` field can be added in the following way:

```shell
python3 main.py \
    --database-name example_database \
    --tables example_table \
    --s3-bucket-name example_s3_bucket \
    --add-validation-field
```

## Cleanup

After translating Timestream for LiveAnalytics data to line protocol, three resources/artifacts will be created:
- An Athena table, containing Timestream for LiveAnalytics data. By default, this is `<Timestream database name>_<Timestream table name>` in the `default` Athena database.
- An Athena table, containing translated line protocol data. By default, this is `lp_<Athena table name>` in the `default` Athena database.
- Line protocol data within your S3 bucket, with the path `<Timestream database name>/<Timestream table name>/line-protocol-output`.

To delete any Athena table, run the following [AWS CLI](https://aws.amazon.com/cli/) command, replacing `<Athena table name>` with the name of the table that you want to delete and `<Athena database name>` with the name of the Athena database that the table resides in:

```shell
aws glue delete-table \
    --database-name <Athena database name> \
    --name <Athena table name>
```

To delete line protocol data within your S3 bucket, run the following AWS CLI command, replacing `<S3 bucket name>` with the name of your S3 bucket, `<Timestream database name>` with the name of your Timestream for LiveAnalytics database, `<Timestream table name>` with the name of your Timestream for LiveAnalytics table, and `<timestamp>` with the timestamp that forms the `unload-<%Y-%m-%d-%H:%M:%S>` path in your S3 bucket:

```shell
aws s3 rm s3://<S3 bucket name>/<Timestream database name>/<Timestream table name>/unload-<timestamp>/line-protocol-output --recursive
```
