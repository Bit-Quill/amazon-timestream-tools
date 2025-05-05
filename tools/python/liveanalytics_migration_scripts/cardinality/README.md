# Cardinality Calculation Script

## Overview

[Cardinality](https://docs.influxdata.com/influxdb/v2/reference/glossary/#series-cardinality) in InfluxDB is the "number of unique measurement, tag set, and field key combinations in an InfluxDB bucket." When migrating from Timestream, carefully select your InfluxDB instance specifications based on your dataset's cardinality as this directly impacts performance and resource requirements and consider migrating to a destination other than InfluxDB if your cardinality is more than ten million. Refer to [Timestream for InfluxDB's documentation on cardinality management](https://docs.aws.amazon.com/timestream/latest/developerguide/timestream-for-influxdb.html#timestream-for-influx-getting-started-security-best-practices) to understand how exceeding recommended limits can degrade query performance and increase memory consumption. Benchmark your anticipated query patterns against representative data samples before finalizing your instance selection to ensure your analytics remain performant post-migration, paying particular attention to memory-intensive aggregation queries that might behave differently than in Timestream.

This script calculates the cardinality of a Timestream for LiveAnalytics table when mapped to Timestream for InfluxDB using the LiveAnalytics migration script. If the cardinality is under **ten million**, you can determine which Timestream for InfluxDB instance type to migrate to, otherwise how to adjust the schema for reducing cardinality. Using the default schema mapping, cardinality is calculated by computing the total unique combinations of dimensions and measure name. The script executes the following query to do this:

```sql
SELECT 
    COUNT(
        DISTINCT(
            measure_name, dimension_name1, dimension_name2, 
            dimension_name3
        )
    ) AS cardinality
FROM 
    "database_name"."table_name"
```

## Prerequisites

The following prerequisites must be met before running the script:
1. [AWS credentials configured for use with boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file).
2. A Timestream for LiveAnalytics table [created](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.table.using-console) and loaded with data.
3. [Python 3.13 installed](https://www.python.org/downloads/).
4. Optionally, a [Python virtual environment](https://docs.python.org/3/library/venv.html), with all packages in `requirements.txt` installed. The following command can be used to create a virtual environment, activate it, and install all necessary packages:
   ```shell
   python3 -m venv env && \
   source env/bin/activate && \
   python3 -m pip install -r requirements.txt
   ```

## Usage

### Options

`cardinality.py` provides the following options:

- `-h`, `--help`: Show this help message and exit.
- `--table-name TABLE_NAME`: The Timestream for LiveAnalytics table to determine the cardinality of.
- `--database-name DATABASE_NAME`: The Timestream for LiveAnalytics database that your table resides in.
- `--exclude-dimensions EXCLUDE_DIMENSIONS`: Optional. A list of dimension names to exclude from the cardinality calculation separated by commas. In a real-world scenario, changing Timestream for LiveAnalytics dimensions to InfluxDB fields rather than InfluxDB tags when translating Timestream for LiveAnalytics records to line protocol lowers the cardinality.

### Basic Usage

To determine the cardinality of a table, example_table, in the database example_database the script can be used in the following way:

```shell
python3 cardinality.py \
    --table-name example_table \
    --database-name example_database
```

This produces the following output:

```console
Cardinality of "example_database"."example_table": 160
Your recommended Timestream for InfluxDB type is: db.influx.medium
```

### Excluding Dimensions

If you plan to later change particular dimensions to fields, for example, `dimension_1` and `dimension_2`, using the Timestream for LiveAnalytics to Line Protocol Translation Script, detailed below, the script provides the `--exclude-dimensions` argument to calculate cardinality if these dimensions were fields. To do this, run the script in the following way:

```shell
python3 cardinality.py \
    --table-name example_table \
    --database-name example_database \
    --exclude-dimensions dimension_1,dimension_2
```

This produces the following output:

```console
Cardinality of "example_database"."example_table": 160
Your recommended Timestream for InfluxDB type is: db.influx.medium
Hypothetical cardinality of "example_database"."example_table" if the
dimensions dimension_1 and dimension_2 became fields: 16
Your hypothetical recommended Timestream for InfluxDB instance type is: db.influx.medium
```
