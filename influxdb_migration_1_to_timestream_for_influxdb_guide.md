# Manually migrate from InfluxDB 1.x to Amazon Timestream for InfluxDB

This is an adaptation of [InfluxData's official guide for migrating from InfluxDB 1.x to InfluxDB 2.7](https://docs.influxdata.com/influxdb/v2/install/upgrade/v1-to-v2/manual-upgrade/). The difference between this guide and that guide is that no InfluxDB 2.x instance is created in this guide. Instead, all data is exported from InfluxDB 1.x and migrated to Timestream for InfluxDB using the Influx CLI and the instance's endpoint and an operator token.

To manually upgrade from InfluxDB 1.x to Amazon Timestream for InfluxDB:

1. [Migrate custom configuration settings](#migrate-custom-configuration-settings).
2. [Create DBRP mappings](#create-dbrp-mappings).
3. [Create authorizations](#create-authorizations).
4. [Migrate time series data](#migrate-time-series-data).
5. [Migrate continuous queries](#migrate-continuous-queries).

## Migrate custom configuration settings

If you’re using custom configuration settings in your InfluxDB 1.x instance, do the following:

  1. Compare 1.x and 2.7 configuration settings:

      <details>
        <summary>View configuration option parity</summary>

      | 1.x configuration option           | Timestream for InfluxDB option             |
      |------------------------------------|--------------------------------------------|
      | [**data**]                         |                                            |
      | dir                                | engine-path                                |
      | wal-dir                            | engine-path                                |
      | wal-fsync-delay                    | storage-wal-fsync-delay                    |
      | index-version                      |                                            |
      | trace-logging-enabled              |                                            |
      | query-log-enabled                  |                                            |
      | strict-error-handling              |                                            |
      | validate-keys                      | storage-validate-keys                      |
      | cache-max-memory-size              | storage-cache-max-memory-size              |
      | cache-snapshot-memory-size         | storage-cache-snapshot-memory-size         |
      | cache-snapshot-write-cold-duration | storage-cache-snapshot-write-cold-duration |
      | compact-full-write-cold-duration   | storage-compact-full-write-cold-duration   |
      | max-concurrent-compactions         | storage-max-concurrent-compactions         |
      | compact-throughput                 |                                            |
      | compact-throughput-burst           | storage-compact-throughput-burst           |
      | tsm-use-madv-willneed              | storage-tsm-use-madv-willneed              |
      | max-series-per-database            |                                            |
      | max-values-per-tag                 |                                            |
      | max-index-log-file-size            | storage-max-index-log-file-size            |
      | series-id-set-cache-size           | storage-series-id-set-cache-size           |
      |                                    |                                            |
      | [**retention**]                    |                                            |
      | check-interval                     | storage-retention-check-interval           |
      |                                    |                                            |
      | [**shard-precreation**]            |                                            |
      | check-interval                     | storage-shard-precreator-check-interval    |
      | advance-period                     | storage-shard-precreator-advance-period    |
      |                                    |                                            |
      | [**http**]                         |                                            |
      | flux-enabled                       |                                            |
      | flux-log-enabled                   | flux-log-enabled                           |
      | bind-address                       | http-bind-address                          |
      | auth-enabled                       |                                            |
      | realm                              |                                            |
      | log-enabled                        |                                            |
      | suppress-write-log                 |                                            |
      | access-log-path                    |                                            |
      | access-log-status-filters          |                                            |
      | write-tracing                      |                                            |
      | pprof-enabled                      | pprof-disabled                             |
      | pprof-auth-enabled                 |                                            |
      | debug-pprof-enabled                |                                            |
      | ping-auth-enabled                  |                                            |
      | https-enabled                      |                                            |
      | https-certificate                  | tls-cert                                   |
      | https-private-key                  | tls-key                                    |
      | shared-secret                      |                                            |
      | max-row-limit                      |                                            |
      | max-connection-limit               |                                            |
      | unix-socket-enabled                |                                            |
      | bind-socket                        |                                            |
      | max-body-size                      |                                            |
      | max-concurrent-write-limit         |                                            |
      | max-enqueued-write-limit           |                                            |
      | enqueued-write-timeout             | http-write-timeout                         |
      |                                    |                                            |
      | [**logging**]                      |                                            |
      | format                             |                                            |
      | level                              | log-level                                  |
      | suppress-logo                      |                                            |
      |                                    |                                            |
      | [**tls**]                          |                                            |
      | ciphers                            | tls-strict-ciphers                         |
      | min-version                        | tls-min-version                            |
      | max-version                        |                                            |

      </details>
  
  2. Apply your 1.x custom settings to the comparable InfluxDB 2.7 settings using `influxd` flags, environment variables, or a 2.7 configuration file. For more information about configuring InfluxDB 2.7, see [Configuration options](https://docs.influxdata.com/influxdb/v2/reference/config-options/).

  3. Restart `influxd`.

## Create DBRP mappings

InfluxDB database and retention policy (DBRP) mappings associate database and retention policy combinations with Timestream for InfluxDB buckets. These mappings allow InfluxDB 1.x clients to successfully query and write to Timestream for InfluxDB buckets while using the 1.x DBRP convention.

For more information about DBRP mapping, see Database and retention policy mapping.

To map a DBRP combination to a Timestream for InfluxDB bucket:

1. Create a bucket
  
    Create a bucket in Timestream for InfluxDB. We recommend creating a bucket for each unique 1.x database and retention policy combination using the following naming convention:

    ```shell
    # Naming convention
    db-name/rp-name

    # Example
    telegraf/autogen
    ```

2. Create a DBRP mapping
    
    Use the `influx v1 dbrp create` command to create a DBRP mapping. Provide the following:

      - Database name.
      - Retention policy name (not retention period).
      - Bucket ID.
      - (Optional) `--default` flag if you want the retention policy to be the default retention policy for the specified database.

    ```shell
    # DB with one RP DB

    influx v1 dbrp create \
      --host <Timestream for InfluxDB endpoint> \
      --token <token> \
      --db example-db \
      --rp example-rp \
      --bucket-id 00xX00o0X001 \
      --default

    # DB with multiple RPs

    # Create telegraf/autogen DBRP mapping with autogen
    # as the default RP for the telegraf DB

    influx v1 dbrp create \
      --host <Timestream for InfluxDB endpoint> \
      --token <token> \
      --db telegraf \
      --rp autogen \
      --bucket-id 00xX00o0X001 \
      --default

    # Create telegraf/downsampled-daily DBRP mapping that
    # writes to a different bucket

    influx v1 dbrp create \
      --host <Timestream for InfluxDB endpoint> \
      --token <token> \
      --db telegraf \
      --rp downsampled-daily \
      --bucket-id 00xX00o0X002
    ```

3. Confirm the DBRP mapping was created
  
   Use the `influx v1 dbrp list` command to list existing DBRP mappings.

   ```shell
   influx v1 dbrp list
   ```

For information about managing DBRP mappings, see the [`influx v1 dbrp` command documentation](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/v1/dbrp/).

## Create authorizations

Timestream for InfluxDB requires authentication and provides two authentication methods:

- [Token authentication](#token-authentication).
- [1.x compatible authorizations](#1x-compatible-authorizations).

### Token authentication

Use Timestream for InfluxDB token authentication to authenticate requests to Timestream for InfluxDB.

Recommended if:

- Your 1.x instance does not have authentication enabled.

> **Use tokens with basic authentication**
> 
> To use tokens with InfluxDB clients that require an InfluxDB username and password, provide an arbitrary user name and pass the token as the password.

### 1.x-compatible authorizations

Timestream for InfluxDB provides a 1.x compatibility API that lets you authenticate using a username and password as in InfluxDB 1.x. If authentication is enabled in your InfluxDB 1.x instance, create a 1.x-compatible authorization with the same username and password as your InfluxDB 1.x instance to allow external clients to connect to your Timestream for InfluxDB instance without any change.

Recommended if:

- Your 1.x instance has authentication enabled.
- You’re using InfluxDB 1.x clients or client libraries configured with InfluxDB usernames and passwords.

> 1.x compatibility authorizations are separate from credentials used to log into the Timestream for InfluxDB user interface (UI).

#### Create a 1.x-compatible authorization

Use the Influx CLI `influx v1 auth create` command to create a 1.x-compatible authorization that grants read/write permissions to specific Timestream for InfluxDB buckets. Provide the following:

- List of bucket IDs to grant read or write permissions to.
- New v1 auth username.
- New v1 auth password (when prompted).

```shell
# Single bucket

influx v1 auth create \
  --host <Timestream for InfluxDB endpoint> \
  --token <token> \
  --read-bucket 00xX00o0X001 \
  --write-bucket 00xX00o0X001 \
  --username example-user

# Multiple buckets

influx v1 auth create \
  --host <Timestream for InfluxDB endpoint> \
  --token <token> \
  --read-bucket 00xX00o0X001 \
  --read-bucket 00xX00o0X002 \
  --write-bucket 00xX00o0X001 \
  --write-bucket 00xX00o0X002 \
  --username example-user
```

For information about managing 1.x compatible authorizations, see the [`influx v1 auth` command documentation](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/v1/auth/).

## Migrate time series data

To migrate time series data from your InfluxDB 1.x instance to Timestream for InfluxDB:

1. On the InfluxDB 1.x instance, use the InfluxDB 1.x [`influx_inspect export` command](https://docs.influxdata.com/influxdb/v1/tools/influx_inspect/#export) to export time series data as line protocol. Include the `-lponly` flag to exclude comments and the data definition language (DDL) from the output file.

   *We recommend exporting each DBRP combination separately to easily write data to a corresponding InfluxDB 2.7 bucket*.

   ```shell
    # Syntax
    influx_inspect export \
      -database <database-name> \
      -retention <retention-policy-name> \
      -out <output-file-path> \
      -lponly

    # Example
    influx_inspect export \
      -database example-db \
      -retention example-rp \
      -out /path/to/example-db_example-rp.lp \
      -lponly
   ```

2. Use the Influx CLI `influx write` command to write the exported line protocol to Timestream for InfluxDB.
   ```shell
    # Syntax
    influx write \
      --host <Timestream for InfluxDB endpoint> \
      --org <org> \
      --token <token>
      --bucket <bucket-name> \
      --file <path-to-line-protocol-file>

    # Example
    influx write \
      --host https://example-host:8086 \
      --org example-org \
      --token dfdsKJnsdkjlsDJFlkjfdnd== \
      --bucket example-db/example-rp \
      --file /path/to/example-db_example-rp.lp
   ```

3. Repeat steps 1-2 for each bucket.

## Migrate continuous queries

For information about migrating InfluxDB 1.x continuous queries to Timestream for InfluxDB tasks, see InfluxData's [Migrate continuous queries to tasks](https://docs.influxdata.com/influxdb/v2/install/upgrade/v1-to-v2/migrate-cqs/) guide.

