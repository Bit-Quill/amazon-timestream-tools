# Tests

## Integration Tests

The directory `integration` contains integration tests.

### Common Tests

`common.py` contains integration tests for scripts that are independent of the migration target, such as `unload.py`. These tests mainly integrate with Timestream for LiveAnalytics.

### InfluxDB V2 Target Tests

`influxdb_v2_target.py` contains end-to-end integration tests for migrating from Timestream for LiveAnalytics to InfluxDB.

#### Setup

The InfluxDB V2 target integration tests require a local InfluxDB instance running on http://localhost:8086. The directory `test_scripts` contains bash scripts that help set up an InfluxDB Docker container with credentials the tests expect. To set up a local InfluxDB Docker container, run the following command from within `test_scripts`:

```shell
./influxdb-restart.sh
```

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
