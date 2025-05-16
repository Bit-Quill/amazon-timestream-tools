from datetime import datetime
import os
import time
import unittest
import random
import string

import boto3
from boto3 import Session
from botocore.client import BaseClient
from botocore.exceptions import ClientError

import pandas
from pandas import Timedelta
from influxdb_client import InfluxDBClient

import unload
from cardinality import cardinality
from targets.timestream_for_influxdb.transform import transform
from targets.timestream_for_influxdb.ingestion import influxdb_ingestion
from targets.timestream_for_influxdb.validation import validator

UNLOAD_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
ISO_8601_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class BaseTestCases:
    class BaseTestCase(unittest.TestCase):
        session: Session
        timestream_write_client: BaseClient
        s3_client: BaseClient
        influxdb_client: InfluxDBClient
        database_name_prefix = "la-idb-it-db"
        database_name: str
        table_name_prefix = "la-idb-it-table"
        table_name: str
        s3_bucket_name_prefix = "la-idb-it-bucket"
        s3_bucket_name: str

        @classmethod
        def setUpClass(cls):
            """
            Overrides unittest.TestCase.setUpClass, called once before any
            tests in the class.
            """
            # InfluxDB setup.
            os.environ["INFLUXDB_V2_URL"] = "http://localhost:8086"
            influxdb_url = "http://localhost:8086"
            cls.influxdb_client = InfluxDBClient(
                url=influxdb_url, token="test-token", org="test-org"
            )
            health_check = cls.influxdb_client.ping()
            if not health_check:
                raise ConnectionError(
                    f"Failed to connect to InfluxDB v2 at {influxdb_url}"
                )

            # AWS setup.
            cls.session = Session()
            cls.timestream_write_client = cls.session.client("timestream-write")
            cls.s3_client = cls.session.client("s3")

            cls.database_name = cls.database_name_prefix + cls.get_random_string(10)
            cls.timestream_write_client.create_database(DatabaseName=cls.database_name)
            # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
            time.sleep(1)
            print("Waiting for database creation")
            cls.wait_for_database_creation(cls.database_name)

            cls.s3_bucket_name = cls.s3_bucket_name_prefix + cls.get_random_string(10)
            cls.s3_client.create_bucket(
                Bucket=cls.s3_bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": cls.session.region_name
                },
            )

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
                        return False
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
                        return True
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    if error_code != "ResourceNotFoundException":
                        return False
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

        @classmethod
        def tearDownClass(cls):
            instance = cls()
            instance.delete_database(database_name=instance.database_name)
            instance.delete_s3_bucket(bucket_name=instance.s3_bucket_name)

        def tearDown(self):
            """
            Overrides unittest.TestCase.tearDown, called after each test runs
            """
            self.timestream_write_client.delete_table(
                DatabaseName=self.database_name, TableName=self.table_name
            )


class TestCase(BaseTestCases.BaseTestCase):
    def test_sm_basic(self):
        # Millisecond timestamp
        current_time = pandas.Timestamp.now()
        start_time = current_time - Timedelta(days=30)
        end_time = start_time + Timedelta(days=1)
        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"}
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
        cardinality.main(
            [
                "--database-name",
                self.database_name,
                "--table-name",
                self.table_name,
                "--start-time",
                start_time.strftime(ISO_8601_FORMAT),
                "--end-time",
                end_time.strftime(ISO_8601_FORMAT),
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

    def test_sm_invalid(self):
        with self.assertRaises(SystemExit) as error:
            exit(1)
        self.assertEqual(error.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
