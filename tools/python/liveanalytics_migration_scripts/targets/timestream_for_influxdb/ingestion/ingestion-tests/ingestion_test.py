"""
Tests for the influxdb_ingestion.py script.

This test suite verifies ingesting gz files into InfluxDB buckets with the influxdb_ingestion.py script.
"""

import os
import sys
import time
import subprocess

# Add the parent directory to the path so we can import the script
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the script to test
import influxdb_ingestion


class TestInfluxDBIngestion:
    """Test suite for InfluxDB ingestion script."""

    def test_valid_dataset_ingestion(self, influxdb_setup, valid_data_dir):
        """
        Test ingestion of the full valid dataset using 11 workers.

        This test:
        1. Creates a 'valid-01' bucket
        2. Ingests all valid test data using 11 workers
        3. Validates ingestion
        """
        # Create the valid-01 bucket
        client = influxdb_setup
        buckets_api = client.buckets_api()
        org = os.environ.get("INFLUXDB_V2_ORG")
        bucket_name = "valid-01"

        try:
            buckets_api.create_bucket(bucket_name=bucket_name, org=org)
            print(f"Created bucket: {bucket_name}")
        except Exception as e:
            if "already exists" in str(e):
                print(f"Bucket {bucket_name} already exists")
            else:
                raise

        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "influxdb_ingestion.py",
        )

        print(f"Starting ingestion of valid dataset to {bucket_name}")
        start_time = time.time()

        result = subprocess.run(
            [
                sys.executable,
                script_path,
                bucket_name,  # bucket name
                valid_data_dir,  # data directory
                "-w",
                "11",  # 11 workers
                "-l",
                "10000",  # lines per batch
                "-m",
                "10",  # IO multiplier
                "-r",
                "5",  # retries
            ],
            capture_output=True,
            text=True,
        )

        end_time = time.time()
        duration = end_time - start_time

        print(f"Ingestion completed in {duration:.2f} seconds")

        assert result.returncode == 0, f"Script failed with output: {result.stderr}"

        query_api = client.query_api()
        query = """
        from(bucket: "valid-01")
          |> range(start: 0)
          |> filter(fn: (r) => r["_field"] == "la_unload")
          |> group()
          |> count()
        """

        print("Executing validation query")
        query_result = query_api.query(query)

        assert len(query_result) > 0, "Query returned no results"

        # Extract the count value from the result
        count_value = None
        for table in query_result:
            for record in table.records:
                count_value = record.get_value()
                break
            if count_value is not None:
                break

        print(f"Query returned count: {count_value}")

        expected_count = 50000250
        assert count_value == expected_count, (
            f"Expected count {expected_count}, got {count_value}"
        )

        print(
            f"Validation successful: count matches expected value of {expected_count}"
        )

    def test_invalid_dataset_ingestion_error(self, influxdb_setup, invalid_data_dir):
        """
        Test ingestion failure with a malformed dataset.

        This test:
        1. Creates an 'invalid-01' bucket
        2. Attempts to ingest data
        3. Verifies the script fails early
        """
        client = influxdb_setup
        buckets_api = client.buckets_api()
        org = os.environ.get("INFLUXDB_V2_ORG")
        bucket_name = "invalid-01"

        try:
            buckets_api.create_bucket(bucket_name=bucket_name, org=org)
            print(f"Created bucket: {bucket_name}")
        except Exception as e:
            if "already exists" in str(e):
                print(f"Bucket {bucket_name} already exists")
            else:
                raise

        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "influxdb_ingestion.py",
        )
        result = subprocess.run(
            [
                sys.executable,
                script_path,
                bucket_name,
                invalid_data_dir,
                "-w",
                "5",  # workers
                "-l",
                "10000",  # lines per batch
                "-m",
                "10",  # IO multiplier
                "-r",
                "3",  # retries
            ],
            capture_output=True,
            text=True,
        )

        # The script should return non-zero if encountering an error and continue-on-error is not set
        assert result.returncode != 0, f"Script failed with output: {result.stderr}"
        print(
            "Successfully stopped ingestion when reaching error and not using continue-on-error flag"
        )

    def test_invalid_dataset_ingestion_continue_on_error(
        self, influxdb_setup, invalid_data_dir
    ):
        """
        Test ingestion with a malformed dataset and using continue-on-error flag.

        This test:
        1. Creates an 'invalid-02' bucket
        2. Attempts to ingest malformed data with continue-on-error flag set
        3. Verifies all non-malformed data is ingested
        """

        client = influxdb_setup
        buckets_api = client.buckets_api()
        org = os.environ.get("INFLUXDB_V2_ORG")
        bucket_name = "invalid-02"

        try:
            buckets_api.create_bucket(bucket_name=bucket_name, org=org)
            print(f"Created bucket: {bucket_name}")
        except Exception as e:
            if "already exists" in str(e):
                print(f"Bucket {bucket_name} already exists")
            else:
                raise

        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "influxdb_ingestion.py",
        )

        print(
            f"Starting ingestion with invalid dataset to {bucket_name} using 5 workers"
        )
        start_time = time.time()

        result = subprocess.run(
            [
                sys.executable,
                script_path,
                bucket_name,
                invalid_data_dir,
                "-w",
                "5",  # workers
                "-l",
                "10000",  # lines per batch
                "-m",
                "10",  # IO multiplier
                "-r",
                "3",  # retries
                "--continue-on-error",  # continue on error flag
            ],
            capture_output=True,
            text=True,
        )

        end_time = time.time()
        duration = end_time - start_time

        print(f"Ingestion completed in {duration:.2f} seconds")

        query_api = client.query_api()
        query = """
        from(bucket: "invalid-01")
          |> range(start: 0)
          |> filter(fn: (r) => r["_field"] == "la_unload")
          |> group()
          |> count()
        """

        print("Executing validation query")
        query_result = query_api.query(query)

        assert len(query_result) > 0, "Query returned no results"

        # Extract the count value from the result
        count_value = None
        for table in query_result:
            for record in table.records:
                count_value = record.get_value()
                break
            if count_value is not None:
                break

        print(f"Query returned count: {count_value}")

        # The script should return zero if continue-on-error is working
        assert result.returncode == 0, f"Script failed with output: {result.stderr}"

        complete_dataset_count = 50000250
        # File test-data/invalid/influxdb_data_05.gz is malformed and has a line count of 5000000
        malformed_file_count = 5000000
        assert (
            count_value is not None
            and count_value < complete_dataset_count
            and count_value > (complete_dataset_count - malformed_file_count)
        ), f"Unexpected count {count_value}"

        print(
            "Successfully ingested all non-malformed data using continue-on-error flag"
        )

    def test_check_bucket_exists(self, influxdb_setup):
        """Test that the bucket existence check works correctly."""
        # Should return True for existing buckets
        assert influxdb_ingestion.check_bucket_exists("testbucket") is True

        # Should return False for non-existent buckets
        assert (
            influxdb_ingestion.check_bucket_exists("nonexistent_bucket") is False
        )

    def test_decompress_gzip_file(self, valid_data_dir, tmp_path):
        """Test that gzip files can be decompressed correctly."""
        # Copy a test file to a temporary directory
        test_file = os.path.join(
            valid_data_dir, "influxdb_data_11.gz"
        )  # Using the smallest file
        test_file_copy = os.path.join(tmp_path, "test_file.gz")

        with open(test_file, "rb") as src, open(test_file_copy, "wb") as dst:
            dst.write(src.read())
        extracted_path = influxdb_ingestion.decompress_gzip_file(test_file_copy)

        assert os.path.exists(extracted_path)

        with open(extracted_path, "r") as f:
            content = f.read()
            assert len(content) > 0
