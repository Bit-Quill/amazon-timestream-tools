from datetime import datetime
import glob
import os
import shutil
import time
import unittest
import random
import string

from boto3 import Session
from botocore.client import BaseClient

import pandas
from pandas import Timedelta
from influxdb_client import InfluxDBClient

import unload
import cardinality
import targets.timestream_for_influxdb.transform
import targets.timestream_for_influxdb.ingestion
import targets.timestream_for_influxdb.validation


class BaseTestCases:
    class BaseTestCase(unittest.TestCase):
        session: Session
        timestream_write_client: BaseClient
        influxdb_client: InfluxDBClient
        database_name_prefix = "la-idb-it-db"
        database_name: str
        table_name_prefix = "la-idb-it-table"
        table_name: str

        @classmethod
        def setUpClass(cls):
            """
            Overrides unittest.TestCase.setUpClass, called once before any
            tests in the class.
            """
            cls.session = Session()
            cls.timestream_write_client = cls.session.client("timestream-write")
            influxdb_url = "http://localhost:8086"
            cls.influxdb_client = InfluxDBClient(
                url=influxdb_url, token="test-token", org="test-org"
            )
            cls.database_name = cls.database_name_prefix + cls.get_random_string(10)
            cls.timestream_write_client.create_database(DatabaseName=cls.database_name)
            health_check = cls.influxdb_client.ping()
            if not health_check:
                raise ConnectionError(
                    f"Failed to connect to InfluxDB v2 at {influxdb_url}"
                )

        def setUp(self):
            """
            Overrides unittest.TestCase.setUp, called before each test runs.
            """
            self.table_name = self.table_name_prefix + self.get_random_string(10)
            self.timestream_write_client.create_table(
                DatabaseName=self.database_name, TableName=self.table_name
            )

        @staticmethod
        def get_random_string(length: int):
            return "".join(
                random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                for _ in range(length)
            )

        def put_records(self, records: list | dict) -> dict:
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

            for table_name in table_names:
                self.timestream_write_client.delete_table(DatabaseName=database_name, TableName=table_name)
            self.timestream_write_client.delete_database(DatabaseName=database_name)

        @classmethod
        def tearDownClass(cls):
            cls.delete_database(self=cls, database_name=cls.database_name)

        def tearDown(self):
            """
            Overrides unittest.TestCase.tearDown, called after each test runs
            """
            self.delete_database(database_name=self.database_name)


class TestCase(BaseTestCases.BaseTestCase):
    def test_basic(self):
        assert True


if __name__ == "__main__":
    unittest.main()
