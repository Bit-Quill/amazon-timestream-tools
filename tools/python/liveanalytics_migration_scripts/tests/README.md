# Tests

## Integration Tests

The directory `integration` contains integration tests.

### Prerequisites

1. AWS CLI configured with appropriate permissions.
2. Python 3.12+.
3. Required Python packages (see [test-requirements.txt](test-requirements.txt)).

Additionally, for the InfluxDB V2 target tests, a local InfluxDB instance must be up and running on http://localhost:8086. The directory `test_scripts` contains bash scripts that help set up an InfluxDB Docker container with credentials the tests expect.

## Installation

Create a virtual environment using `venv` and install required dependencies:

```shell
python3 -m venv env && \
source env/bin/activate && \
python3 -m pip install -r test-requirements.txt
```

For the InfluxDB V2 target tests, run the following command from within `test_scripts` to set up a local InfluxDB Docker container:

```shell
./influxdb-restart.sh
```

### Common Tests

`common.py` contains integration tests for scripts that are independent of the migration target, such as `unload.py`. These tests mainly integrate with Timestream for LiveAnalytics.

### InfluxDB V2 Target Tests

`influxdb_v2_target.py` contains end-to-end integration tests for migrating from Timestream for LiveAnalytics to InfluxDB.

### Running All Tests

Assuming you have satisfied the prerequisites for all integration tests, such as having installed all packages from `../requirements.txt`, all tests can be run with the following command:

```
python3 -m pytest *
```

### Running a Specific Test

A specific test can be run with the following command, replacing `<test file name>` with the name of the test file that you want to run, `<test case name>` with the name of the test case name that you want to run (such as `InfluxDBV2TestCase`), and `<test name>` with the name of the test that you want to run:

```
python3 -m pytest <test file name>.py::<test case name>::<test name>
```

### Reducing Test Verbosity

A `pytest.ini` file is provided with some default configurations. By default, tests show `INFO` logs and output from print statements. To disable this, use the following command, replacing `<test file name>` with the name of the test file that you want to run:

```
python3 -m pytest <test file name>.py \
    --override-ini="log_cli=false" \
    --override-ini="addopts="
```
