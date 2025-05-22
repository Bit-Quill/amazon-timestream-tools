import os
import time
import unittest
import random
import string
import shutil
import sys
import logging

from boto3 import Session
from botocore.client import BaseClient
from botocore.exceptions import ClientError

import pandas
from pandas import Timedelta
import pytest
from influxdb_client import InfluxDBClient

sys.path.append("../../")

import unload
from unload.utils.s3_utils import S3Utility
from cardinality import cardinality
from targets.timestream_for_influxdb.transform import transform
from targets.timestream_for_influxdb.ingestion import influxdb_ingestion
from targets.timestream_for_influxdb.validation import validator

# Format expected by unload.py.
UNLOAD_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
# Format expected by the cardinality and validation scripts.
ISO_8601_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class BaseTestCases:
    class BaseTestCase(unittest.TestCase):
        session: Session
        timestream_write_client: BaseClient
        s3_client: BaseClient
        s3_utility: S3Utility
        glue_client: BaseClient
        influxdb_client: InfluxDBClient

        influxdb_bucket_name_prefix = "la-idb-it-lp-influxdb-bucket"
        influxdb_bucket_name: str

        # Directory for all tests to unload lp data into.
        lp_base_directory = "la-idb-it-lp-output-base"
        # Unique subdirectory for a tests's lp output.
        lp_directory_prefix = "la-idb-it-lp-output"
        lp_directory: str

        # Prefixes stand for
        # "LiveAnalytics InfluxDB Integration Test".
        database_name_prefix = "la-idb-it-db-"
        database_name: str

        table_name_prefix = "la-idb-it-table-"
        table_name: str

        s3_bucket_name_prefix = "la-idb-it-bucket-"
        s3_bucket_name: str

        athena_database_name = "default"
        athena_table_name: str
        athena_lp_table_name: str

        # Whether to silence warnings logs during the cleanup process.
        # Some tests end early or purposely raise exceptions, causing
        # the cleanup process to encounter deletion failures.
        silence_cleanup_logging = False

        @classmethod
        def setUpClass(cls):
            """
            Overrides unittest.TestCase.setUpClass, called once before any
            tests in the class.
            """
            # InfluxDB setup.
            # These values are assumed by the configuration in the
            # test_scripts directory.
            influxdb_url = "http://localhost:8086"

            # The ingestion scripts requires these environment variables.
            os.environ["INFLUXDB_V2_URL"] = influxdb_url
            os.environ["INFLUXDB_V2_ORG"] = "test-org"
            os.environ["INFLUXDB_V2_TOKEN"] = "test-token"

            cls.influxdb_client = InfluxDBClient.from_env_properties()
            health_check = cls.influxdb_client.ping()
            if not health_check:
                raise ConnectionError(
                    f"setUpClass: Failed to connect to InfluxDB v2 at {influxdb_url}"
                )

            # AWS setup.
            cls.session = Session()
            cls.timestream_write_client = cls.session.client("timestream-write")
            cls.s3_client = cls.session.client("s3")

            cls.s3_utility = S3Utility()

            # For interacting with Athena.
            cls.glue_client = cls.session.client("glue")

            cls.database_name = cls.database_name_prefix + cls.get_random_string(10)
            cls.timestream_write_client.create_database(DatabaseName=cls.database_name)
            # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
            time.sleep(1)
            cls.wait_for_database_creation(cls.database_name)

            # Create directory to hold nested directories of line protocol data.
            os.makedirs(cls.lp_base_directory, exist_ok=True)

        def setUp(self):
            """
            Overrides unittest.TestCase.setUp, called before each test runs.
            """
            self.table_name = self.table_name_prefix + self.get_random_string(10)
            time.sleep(1)
            self.timestream_write_client.create_table(
                DatabaseName=self.database_name,
                TableName=self.table_name,
                RetentionProperties={
                    "MemoryStoreRetentionPeriodInHours": 8766,
                    "MagneticStoreRetentionPeriodInDays": 7305,
                },
            )
            self.wait_for_table_creation(
                database_name=self.database_name, table_name=self.table_name
            )
            # Keep a copy of the names of Athena tables that transform will create.
            # This should be overridden if --athena-table-name is used.
            self.athena_table_name = (
                self.database_name.replace("-", "_")
                + "_"
                + self.table_name.replace("-", "_")
            )
            self.athena_lp_table_name = f"lp_{self.athena_table_name}"

            self.lp_directory = f"{self.lp_base_directory}/{self.lp_directory_prefix + self.get_random_string(10)}"
            os.makedirs(self.lp_directory, exist_ok=True)

            self.influxdb_bucket_name = (
                self.influxdb_bucket_name_prefix + self.get_random_string(10)
            )
            self.influxdb_client.buckets_api().create_bucket(
                bucket_name=self.influxdb_bucket_name
            )

            self.s3_bucket_name = self.s3_bucket_name_prefix + self.get_random_string(
                10
            )
            self.s3_client.create_bucket(
                Bucket=self.s3_bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": self.session.region_name
                },
            )

            self.silence_cleanup_logging = False

        @classmethod
        def wait_for_database_creation(
            cls, database_name: str, max_attempts=20, delay_seconds=5
        ):
            attempts = 0
            while attempts < max_attempts:
                try:
                    cls.timestream_write_client.describe_database(
                        DatabaseName=database_name
                    )
                    return True
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    if error_code != "ResourceNotFoundException":
                        raise
                attempts += 1
                time.sleep(delay_seconds)

        def wait_for_table_creation(
            self, database_name: str, table_name: str, max_attempts=20, delay_seconds=5
        ):
            attempts = 0
            while attempts < max_attempts:
                try:
                    response = self.timestream_write_client.describe_table(
                        DatabaseName=database_name, TableName=table_name
                    )
                    status = response.get("Table", {}).get("TableStatus")
                    if status == "ACTIVE":
                        return
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    if error_code != "ResourceNotFoundException":
                        raise
                attempts += 1
                time.sleep(delay_seconds)

        @staticmethod
        def get_random_string(length: int):
            return "".join(
                random.SystemRandom().choice(string.ascii_lowercase + string.digits)
                for _ in range(length)
            )

        def put_records(self, records: list) -> dict:
            return self.timestream_write_client.write_records(
                DatabaseName=self.database_name,
                TableName=self.table_name,
                Records=records,
            )

        def set_table_name(self, table_name):
            self.table_name = table_name

        def set_database_name(self, database_name):
            self.database_name = database_name

        def delete_database(self, database_name):
            list_tables_response = self.timestream_write_client.list_tables(
                DatabaseName=database_name
            )
            table_names = list_tables_response.get("Tables", [])
            if not table_names and "NextToken" in list_tables_response:
                next_token = list_tables_response["NextToken"]
                while next_token is not None:
                    time.sleep(5)
                    list_tables_response = self.timestream_write_client.list_tables(
                        DatabaseName=database_name
                    )
                    next_token = list_tables_response.get("NextToken", None)
                    table_names.extend(list_tables_response.get("Tables", []))

            for table in table_names:
                self.timestream_write_client.delete_table(
                    DatabaseName=database_name, TableName=table["TableName"]
                )
            self.timestream_write_client.delete_database(DatabaseName=database_name)

        def delete_s3_bucket(self, bucket_name: str):
            object_response_paginator = self.s3_client.get_paginator("list_objects_v2")
            for object_response in object_response_paginator.paginate(
                Bucket=bucket_name
            ):
                if "Contents" in object_response:
                    objects_to_delete = [
                        {"Key": obj["Key"]}
                        for obj in object_response.get("Contents", [])
                    ]
                    if objects_to_delete:
                        self.s3_client.delete_objects(
                            Bucket=bucket_name, Delete={"Objects": objects_to_delete}
                        )
            self.s3_client.delete_bucket(Bucket=bucket_name)

        def delete_athena_tables(
            self, athena_database_name: str, athena_table_names: list
        ):
            for athena_table_name in athena_table_names:
                self.glue_client.delete_table(
                    DatabaseName=athena_database_name, Name=athena_table_name
                )

        @classmethod
        def tearDownClass(cls):
            """
            Overrides unittest.TestCase.tearDownClass, called after all tests have finished.
            """
            instance = cls()
            try:
                # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
                time.sleep(1)
                instance.delete_database(database_name=instance.database_name)
            except Exception as e:
                if not cls.silence_cleanup_logging:
                    logging.warning(
                        f"tearDownClass: Failed to delete Timestream database: {e}"
                    )

            try:
                if os.path.exists(cls.lp_base_directory):
                    shutil.rmtree(cls.lp_base_directory)
            except Exception as e:
                if not cls.silence_cleanup_logging:
                    logging.warning(
                        f"tearDownClass: Failed to delete local line protocol base directory: {e}"
                    )

        def tearDown(self):
            """
            Overrides unittest.TestCase.tearDown, called after each test runs.
            """
            # Tests may purposely fail, causing resource to not be created.
            # To handle this, each resource needs its own try except block
            # with logging in case of failure.
            try:
                # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
                time.sleep(1)
                self.timestream_write_client.delete_table(
                    DatabaseName=self.database_name, TableName=self.table_name
                )
            except Exception as e:
                if not self.silence_cleanup_logging:
                    logging.warning(f"tearDown: Failed to delete Timestream table: {e}")

            try:
                self.delete_athena_tables(
                    athena_database_name=self.athena_database_name,
                    athena_table_names=[self.athena_table_name],
                )
            except Exception as e:
                if not self.silence_cleanup_logging:
                    logging.warning(
                        f"tearDown: Failed to delete Athena unload table: {e}"
                    )

            try:
                self.delete_athena_tables(
                    athena_database_name=self.athena_database_name,
                    athena_table_names=[self.athena_lp_table_name],
                )
            except Exception as e:
                if not self.silence_cleanup_logging:
                    logging.warning(
                        f"tearDown: Failed to delete Athena line protocol table: {e}"
                    )

            try:
                self.delete_s3_bucket(bucket_name=self.s3_bucket_name)
            except Exception as e:
                if not self.silence_cleanup_logging:
                    logging.warning(f"tearDown: Failed to delete S3 bucket: {e}")

            try:
                if os.path.exists(self.lp_directory):
                    shutil.rmtree(self.lp_directory)
            except Exception as e:
                if not self.silence_cleanup_logging:
                    logging.warning(
                        f"tearDown: Failed to delete local line protocol directory: {e}"
                    )

            try:
                influxdb_bucket = (
                    self.influxdb_client.buckets_api().find_bucket_by_name(
                        self.influxdb_bucket_name
                    )
                )
                self.influxdb_client.buckets_api().delete_bucket(influxdb_bucket)
            except Exception as e:
                if not self.silence_cleanup_logging:
                    logging.warning(f"tearDown: Failed to delete InfluxDB bucket: {e}")


class MigrationTest(BaseTestCases.BaseTestCase):
    def test_single_measure_basic(self):
        current_time = pandas.Timestamp.now()
        start_time = current_time - Timedelta(days=30)
        end_time = start_time + Timedelta(days=1)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]
        schema_tags = ",".join(dimension["Name"] for dimension in dimensions)

        record = {
            "Dimensions": dimensions,
            "MeasureName": "cpu_utilization",
            "MeasureValue": "13.5",
            "MeasureValueType": "DOUBLE",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
        }
        self.put_records([record])
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
            ]
        )
        cardinality.main(
            [
                "--database-name",
                self.database_name,
                "--table-name",
                self.table_name,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
            ]
        )
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    def test_single_measure_boolean(self):
        current_time = pandas.Timestamp.now()
        start_time = current_time - Timedelta(days=30)
        end_time = start_time + Timedelta(days=1)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]
        schema_tags = ",".join(dimension["Name"] for dimension in dimensions)

        record = {
            "Dimensions": dimensions,
            "MeasureName": "is_success",
            "MeasureValue": "TRUE",
            "MeasureValueType": "BOOLEAN",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
        }
        self.put_records([record])
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
            ]
        )
        cardinality.main(
            [
                "--database-name",
                self.database_name,
                "--table-name",
                self.table_name,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
            ]
        )
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    @pytest.mark.skip(reason="Nanosecond timestamp precision isn't supported")
    def test_single_measure_nanosecond_timestamp_sequential(self):
        """
        Tests migrating a dataset that is comprised of two records where the
        two records have the same dimensions and measure names but different
        measure values and are one nanosecond apart.

        In InfluxDB, if these data points had the same timestamp, possibly
        due to Timestreamp precision loss, one would override the other,
        causing only one data point to exist in InfluxDB.
        """
        current_time = pandas.Timestamp.now()
        start_time = current_time - Timedelta(days=30)
        end_time = start_time + Timedelta(days=1)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        records = [
            {
                "Dimensions": dimensions,
                "MeasureName": "cpu_utilization",
                "MeasureValue": "13.5",
                "MeasureValueType": "DOUBLE",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
            },
            {
                "Dimensions": dimensions,
                "MeasureName": "cpu_utilization",
                "MeasureValue": "44.0",
                "MeasureValueType": "DOUBLE",
                "Time": str((start_time + Timedelta(nanoseconds=1)).value),
                "TimeUnit": "NANOSECONDS",
            },
        ]
        self.put_records(records)

        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
            ]
        )
        cardinality.main(
            [
                "--database-name",
                self.database_name,
                "--table-name",
                self.table_name,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
            ]
        )
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "timestream",
                "--timestream-database-name",
                self.database_name,
                "--timestream-table-name",
                self.table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    @pytest.mark.skip(reason="Timestamp measures are not supported")
    def test_multi_measure_timestamp_measure(self):
        current_time = pandas.Timestamp.now()
        start_time = current_time - Timedelta(days=30)
        end_time = start_time + Timedelta(days=1)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]
        schema_tags = ",".join(dimension["Name"] for dimension in dimensions)

        # Only multi-measure records can use TIMESTAMP as a measure type.
        record = {
            "Dimensions": dimensions,
            "MeasureName": "metrics",
            "MeasureValueType": "MULTI",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
            "MeasureValues": [
                {
                    "Name": "request_time",
                    "Value": str(start_time.value),
                    "Type": "TIMESTAMP",
                }
            ],
        }
        self.put_records([record])
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
            ]
        )
        cardinality.main(
            [
                "--database-name",
                self.database_name,
                "--table-name",
                self.table_name,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
            ]
        )
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    def test_multi_measure_basic(self):
        current_time = pandas.Timestamp.now()
        start_time = current_time - Timedelta(days=30)
        end_time = start_time + Timedelta(days=1)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]
        schema_tags = ",".join(dimension["Name"] for dimension in dimensions)

        record = {
            "Dimensions": dimensions,
            "MeasureName": "metrics",
            "MeasureValueType": "MULTI",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
            "MeasureValues": [
                {"Name": "cpu_utilization", "Value": "12.3", "Type": "DOUBLE"},
                {"Name": "memory_utilization", "Value": "33.8", "Type": "DOUBLE"},
            ],
        }
        self.put_records([record])
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
            ]
        )
        cardinality.main(
            [
                "--database-name",
                self.database_name,
                "--table-name",
                self.table_name,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
            ]
        )
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    def test_single_measure_start_time_before_end_time(self):
        self.silence_cleanup_logging = True
        with self.assertRaises(Exception):
            current_time = pandas.Timestamp.now()
            end_time = current_time - Timedelta(days=30)
            start_time = end_time + Timedelta(days=1)

            dimensions = [
                {
                    "Name": "hostname",
                    "Value": "hostname1",
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "region",
                    "Value": "us-west-2",
                    "DimensionValueType": "VARCHAR",
                },
            ]

            record = {
                "Dimensions": dimensions,
                "MeasureName": "cpu_utilization",
                "MeasureValue": "13.5",
                "MeasureValueType": "DOUBLE",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
            }
            self.put_records([record])
            unload.main(
                [
                    "--database",
                    self.database_name,
                    "--table",
                    self.table_name,
                    "--s3-uri",
                    f"s3://{self.s3_bucket_name}",
                    "--start-time",
                    start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                    "--end-time",
                    end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                    "--export-table",
                ]
            )


if __name__ == "__main__":
    unittest.main()
