use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use aws_types::region::Region;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{error, info};
use rayon::prelude::{ParallelIterator, ParallelSlice};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Semaphore;
use tokio::task;

/// The maximum number of threads to use for ingesting
/// batches of records to Timestream in parallel.
static NUM_TIMESTREAM_INGEST_THREADS: usize = 12;

pub const DIMENSION_PARTITION_KEY_TYPE: &str = "dimension";
pub const MEASURE_PARTITION_KEY_TYPE: &str = "measure";

/// Environment variables for Timestream for LiveAnalytics.
#[derive(Debug, Clone)]
pub struct TimestreamEnvConfig {
    // Required environment variables
    /// The Timestream for LiveAnalytics database name to use.
    pub database_name: String,
    /// Whether to allow database creation upon ingestion of records.
    pub enable_database_creation: bool,
    /// Whether to allow table creation upon ingestion of records. When using
    // multi-table multi measure schema, each unique line protocol measurement
    /// in a request will result in the creation of a new table with the same
    /// name as the measurement.
    pub enable_table_creation: bool,
    /// Whether to enable magnetic storage writes for the Timestream table.
    pub enable_mag_store_writes: bool,
    /// Whether to only allow the ingestion of records that contain the custom
    /// partition key.
    pub enforce_custom_partition_key: bool,
    /// The AWS region to use, for example, us-west-2.
    pub region: String,
    /// Maps records ingested to a single table or multiple tables.
    /// Valid options are single-table or multi-table.
    pub table_mapping: String,

    // Optional environment variables
    /// The dimension to use as the partition key. This environment variable is
    /// required if the custom_partition_key_type environment variable is set
    /// to 'dimension'.
    pub custom_partition_key_dimension: Option<String>,
    /// The type of custom partition key to use. Valid options are 'dimension'
    /// or 'measure'. The 'dimension' option requires the
    /// custom_partition_key_dimension environment variable to also be set. If
    /// this parameter is not provided, newly-created tables will use default
    // partitioning and none of the parameters relating to custom partition
    /// keys will be used.
    pub custom_partition_key_type: Option<String>,
    /// A comma-separated string of key-value pairs to label the database.
    ///
    /// # Examples
    ///
    /// ```bash
    /// export database_tags='example_key1=example_value1,example_key2=example_value2'
    /// ```
    pub database_tags: Option<Vec<timestream_write::types::Tag>>,
    /// AWS KMS key for the database. If the KMS key is not specified, the
    /// database will be encrypted with a Timestream-managed KMS key located in
    /// your account.
    pub kms_key_id: Option<String>,
    /// The number of days to retain data in magnetic storage in Timestream.
    pub mag_store_retention_period: Option<i64>,
    /// The measure name to use for multi-measure records, when table_mapping
    /// is set to 'multi-table'.
    pub measure_name_for_multi_measure_records: Option<String>,
    /// The number of hours to retain data in memory in Timestream.
    pub mem_store_retention_period: Option<i64>,
    /// Name of the table when table_mapping is set to single-table.
    pub single_table_name: Option<String>,
    /// A comma-separated string of key-value pairs to label the table(s).
    ///
    /// # Examples
    ///
    /// ```bash
    /// export table_tags='example_key1=example_value1,example_key2=example_value2'
    /// ```
    pub table_tags: Option<Vec<timestream_write::types::Tag>>,
}

impl TimestreamEnvConfig {
    fn new() -> Result<Self, Error> {
        let region = match std::env::var("region") {
            Ok(val) => val,
            Err(_) => {
                let err_message = "region environment variable is not defined";
                error!("{}", err_message);
                return Err(anyhow!(err_message));
            }
        };

        let database_name = match std::env::var("database_name") {
            Ok(val) => val,
            Err(_) => {
                let err_message = "database_name environment variable is not defined";
                error!("{}", err_message);
                return Err(anyhow!(err_message));
            }
        };

        let enable_table_creation = std::env::var("enable_table_creation")
            .map(env_var_to_bool)
            .unwrap_or(false);

        let enable_mag_store_writes = std::env::var("enable_mag_store_writes")
            .map(env_var_to_bool)
            .unwrap_or(false);

        let mag_store_retention_period = std::env::var("mag_store_retention_period")
            .ok()
            .and_then(|val| val.parse::<i64>().ok());

        let mem_store_retention_period = std::env::var("mem_store_retention_period")
            .ok()
            .and_then(|val| val.parse::<i64>().ok());

        if enable_table_creation {
            if mag_store_retention_period.is_none() {
                return Err(anyhow!(
                    "mag_store_retention_period environment variable is not defined"
                ));
            }
            if mem_store_retention_period.is_none() {
                return Err(anyhow!(
                    "mem_store_retention_period environment variable is not defined"
                ));
            }
        }

        let table_mapping = match std::env::var("table_mapping") {
            Ok(val) => val.to_lowercase(),
            Err(_) => {
                let err_message = "table_mapping environment variable is not defined";
                error!("{}", err_message);
                return Err(anyhow!(err_message));
            }
        };

        let single_table_name = std::env::var("single_table_name").ok();
        let measure_name_for_multi_measure_records =
            std::env::var("measure_name_for_multi_measure_records").ok();

        // Validate environment variables for table mapping
        match table_mapping.as_str() {
            "single-table" => {
                if single_table_name.is_none() {
                    return Err(anyhow!(
                        "single_table_name environment variable is not defined"
                    ));
                }
            }
            "multi-table" => {
                if measure_name_for_multi_measure_records.is_none() {
                    return Err(anyhow!(
                    "measure_name_for_multi_measure_records environment variable is not defined"
                ));
                }
            }
            table_mapping => {
                return Err(anyhow!(
                    "{:?} is an invalid value for the table_mapping environment variable",
                    table_mapping
                ))
            }
        }

        // Customer-defined partition key environment variables
        let custom_partition_key_type = std::env::var("custom_partition_key_type").ok();
        let custom_partition_key_dimension = std::env::var("custom_partition_key_dimension").ok();
        let enforce_custom_partition_key = std::env::var("enforce_custom_partition_key")
            .map(env_var_to_bool)
            .unwrap_or(false);

        if custom_partition_key_type.is_some() {
            // Check required environment variables for when custom partition key type is "dimension." If it is "measure,"
            // no other environment variables are necessary.
            let custom_partition_key_type_value = match custom_partition_key_type.clone() {
                Some(val) => val,
                None => {
                    let err_message = "Failed to get custom_partition_key_type value";
                    error!("{}", err_message);
                    return Err(anyhow!(err_message));
                }
            };

            if custom_partition_key_type_value == DIMENSION_PARTITION_KEY_TYPE
                && custom_partition_key_dimension.is_none()
            {
                return Err(anyhow!(
                format!("If custom_partition_key_type is {DIMENSION_PARTITION_KEY_TYPE}, then custom_partition_key_dimension must be defined")
            ));
            }
        }

        let database_tags = std::env::var("database_tags")
            .ok()
            .and_then(|database_tags| parse_tags_from_str(&database_tags).ok());

        let table_tags = std::env::var("table_tags")
            .ok()
            .and_then(|table_tags| parse_tags_from_str(&table_tags).ok());

        let enable_database_creation = std::env::var("enable_database_creation")
            .map(env_var_to_bool)
            .unwrap_or(false);

        let kms_key_id = std::env::var("kms_key_id").ok();

        Ok(Self {
            custom_partition_key_dimension,
            custom_partition_key_type,
            database_name,
            database_tags,
            enable_database_creation,
            enable_mag_store_writes,
            enable_table_creation,
            enforce_custom_partition_key,
            kms_key_id,
            mag_store_retention_period,
            measure_name_for_multi_measure_records,
            mem_store_retention_period,
            region,
            single_table_name,
            table_mapping,
            table_tags,
        })
    }
}

/// Timestream environment variable configuration, making sure that environment
/// variables are read once.
/// This being a OnceLock<Mutex<Option<Result<TimestreamEnvConfig, Error>>>>
/// means that it can handle checking for required environment variables and is
/// thread safe.
static TIMESTREAM_ENV_CONFIG: OnceLock<Mutex<Option<Result<TimestreamEnvConfig, Error>>>> =
    OnceLock::new();

/// Gets TIMESTREAM_ENV_CONFIG.
///
/// # Examples
///
/// ```
/// // The following is an efficient way to use LIB_ENV_CONFIG and get a
/// // usable &LibEnvConfig.
/// let timestream_env_config = get_timestream_env_config().map_err(|err| anyhow!(err))?;
/// ```
pub fn get_timestream_env_config() -> Result<TimestreamEnvConfig, Error> {
    let config_lock = TIMESTREAM_ENV_CONFIG.get_or_init(|| Mutex::new(None));
    let mut config = config_lock
        .lock()
        .map_err(|err| anyhow!("Failed to acquire lock: {}", err))?;

    if config.is_none() {
        *config = Some(TimestreamEnvConfig::new());
    }

    match config.take() {
        Some(Ok(env_config)) => Ok(env_config),
        Some(Err(err)) => Err(anyhow!("{}", err)),
        None => Err(anyhow!(
            "Timestream environment configuration has not been initialized"
        )),
    }
}

/// Resets TIMESTREAM_ENV_CONFIG, requiring it to be reinitialized for any future use.
/// This function may return an Err, if a lock cannot be acquired on
/// TIMESTREAM_ENV_CONFIG.
///
/// # Examples
///
/// ```
/// // Here, reset_timestream_env_config is used to reset environment variables during
/// // testing.
/// // Changes to environment variables are only picked up by the connector after
/// // reset_timestream_env_config is called
/// #[tokio::test]
/// async fn test_example() -> Result<(), Error> {
///    env::remove_var("some_environment_variable_removed_for_test");
///    reset_timestream_env_config()?;
///    // . . . test code
///    Ok(())
/// }
/// ```
pub fn reset_timestream_env_config() -> Result<()> {
    if let Some(config_lock) = TIMESTREAM_ENV_CONFIG.get() {
        let mut config = config_lock
            .lock()
            .map_err(|err| anyhow!("Failed to acquire lock: {}", err))?;
        *config = None;
    } else {
        return Err(anyhow!("TIMESTREAM_ENV_CONFIG was not initialized"));
    }

    Ok(())
}

#[derive(Debug)]
pub struct TableConfig {
    pub mag_store_retention_period: i64,
    pub mem_store_retention_period: i64,
    pub enable_mag_store_writes: bool,
    pub enforce_custom_partition_key: Option<timestream_write::types::PartitionKeyEnforcementLevel>,
    pub custom_partition_key_type: Option<timestream_write::types::PartitionKeyType>,
    pub custom_partition_key_dimension: Option<String>,
}

/// Converts an environment variable to a boolean value.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn env_var_to_bool(env_var: String) -> bool {
    matches!(env_var.to_lowercase().as_str(), "true" | "t" | "1")
}

/// Gets a connection to Timestream.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn get_connection(
    region: &str,
) -> Result<timestream_write::Client, timestream_write::Error> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .load()
        .await;
    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failed to get the write client connection with Timestream");

    tokio::task::spawn(reload.reload_task());
    info!("Initialized connection to Timestream in region {}", region);
    Ok(client)
}

/// Creates a new Timestream database.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn create_database(
    client: &Arc<timestream_write::Client>,
    database_name: &str,
    kms_key_id: Option<&str>,
    tags: Option<Vec<timestream_write::types::Tag>>,
) -> Result<(), timestream_write::Error> {
    info!("Creating new database: {}", database_name);

    let mut create_db_builder = client
        .create_database()
        .set_database_name(Some(database_name.to_owned()));

    if let Some(kms) = kms_key_id.filter(|s| !s.is_empty()) {
        info!("Using KMS Key ID: {}", kms);
        create_db_builder = create_db_builder.set_kms_key_id(Some(kms.to_owned()));
    } else {
        info!("No KMS Key ID provided. Using default Timestream-managed KMS key.");
    }

    if let Some(tags_vec) = tags {
        if !tags_vec.is_empty() {
            info!("Adding {} tags to the database.", tags_vec.len());
            create_db_builder = create_db_builder.set_tags(Some(tags_vec));
        } else {
            info!("Empty tags vector provided. Skipping tag assignment.");
        }
    } else {
        info!("No tags provided for the database.");
    }

    create_db_builder.send().await?;

    info!("Database '{}' created successfully.", database_name);
    Ok(())
}

/// Creates a new Timestream table.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn create_table(
    client: &Arc<timestream_write::Client>,
    database_name: &str,
    table_name: &str,
    table_config: TableConfig,
    tags: Option<Vec<timestream_write::types::Tag>>,
) -> Result<(), timestream_write::Error> {
    info!(
        "Creating new table {} for database {}",
        table_name, database_name
    );

    let retention_properties = timestream_write::types::RetentionProperties::builder()
        .set_magnetic_store_retention_period_in_days(Some(table_config.mag_store_retention_period))
        .set_memory_store_retention_period_in_hours(Some(table_config.mem_store_retention_period))
        .build()?;

    let magnetic_store_properties =
        timestream_write::types::MagneticStoreWriteProperties::builder()
            .set_enable_magnetic_store_writes(Some(table_config.enable_mag_store_writes))
            .build()?;

    // Customer-defined partition key configuration
    let table_schema = if table_config.custom_partition_key_type.is_some() {
        let partition_key = timestream_write::types::PartitionKey::builder()
            .set_type(table_config.custom_partition_key_type)
            .set_name(table_config.custom_partition_key_dimension)
            .set_enforcement_in_record(table_config.enforce_custom_partition_key)
            .build()?;

        Some(
            timestream_write::types::Schema::builder()
                .set_composite_partition_key(Some(vec![partition_key]))
                .build(),
        )
    } else {
        None
    };

    let mut create_table_builder = client
        .create_table()
        .set_schema(table_schema)
        .set_table_name(Some(table_name.to_owned()))
        .set_database_name(Some(database_name.to_owned()))
        .set_retention_properties(Some(retention_properties))
        .set_magnetic_store_write_properties(Some(magnetic_store_properties));

    if let Some(tags_vec) = tags {
        if !tags_vec.is_empty() {
            info!("Adding {} tags to the table.", tags_vec.len());
            create_table_builder = create_table_builder.set_tags(Some(tags_vec));
        } else {
            info!("Empty tags vector provided. Skipping tag assignment.");
        }
    } else {
        info!("No tags provided for the table.");
    }

    create_table_builder.send().await?;

    info!(
        "Table '{}' created successfully in database '{}'.",
        table_name, database_name
    );
    Ok(())
}

/// Checks whether a table already exists.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn table_exists(
    client: &Arc<timestream_write::Client>,
    database_name: &str,
    table_name: &str,
) -> Result<bool, Error> {
    match client
        .describe_table()
        .table_name(table_name)
        .database_name(database_name)
        .send()
        .await
    {
        Ok(_) => Ok(true),
        Err(error) => match error
            .as_service_error()
            .map(|e| e.is_resource_not_found_exception())
        {
            Some(true) => Ok(false),
            _ => Err(anyhow!(error)),
        },
    }
}

/// Checks whether a database already exists.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn database_exists(
    client: &Arc<timestream_write::Client>,
    database_name: &str,
) -> Result<bool, Error> {
    match client
        .describe_database()
        .database_name(database_name)
        .send()
        .await
    {
        Ok(_) => Ok(true),
        Err(error) => match error
            .as_service_error()
            .map(|e| e.is_resource_not_found_exception())
        {
            Some(true) => Ok(false),
            _ => Err(anyhow!(error)),
        },
    }
}

/// Parses a string containing comma-separated tags. For example,
/// example_key1=example_value1,example_key2=example_value2.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn parse_tags_from_str(tags_str: &str) -> Result<Vec<timestream_write::types::Tag>, Error> {
    let mut tags = Vec::new();

    for tag in tags_str.split(',') {
        let tag = tag.trim();
        if tag.is_empty() {
            continue;
        }
        let mut parts = tag.splitn(2, '=');
        let key = parts
            .next()
            .ok_or_else(|| anyhow!("Missing key in tag '{}'", tag))?
            .trim();

        // ensure key is not empty
        if key.is_empty() {
            return Err(anyhow!("Tag key must not be empty in tag '{}'", tag));
        }
        let key = key.to_string();

        // get the value if present; if it's missing or empty, use an empty string.
        let value = parts.next().map(|v| v.trim()).unwrap_or("");

        let tag_instance = timestream_write::types::Tag::builder()
            .key(key)
            .value(value.to_string())
            .build()?;
        tags.push(tag_instance);
    }

    Ok(tags)
}

/// Gets a populated TableConfig struct.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn get_table_config() -> Result<TableConfig, Error> {
    let timestream_env_config = get_timestream_env_config().map_err(|err| anyhow!(err))?;

    let custom_partition_key_type = match &timestream_env_config.custom_partition_key_type {
        Some(custom_partition_key_type_value) => {
            match custom_partition_key_type_value.to_lowercase().as_str() {
                DIMENSION_PARTITION_KEY_TYPE => {
                    Some(timestream_write::types::PartitionKeyType::Dimension)
                }
                MEASURE_PARTITION_KEY_TYPE => {
                    Some(timestream_write::types::PartitionKeyType::Measure)
                }
                _ => None,
            }
        }
        _ => None,
    };

    // If custom_partition_key_type is "dimension", then enforce_custom_partition_key is required (true or false).
    // If custom_partition_key_type is "measure", then this will ignore enforce_custom_partition_key.
    // The SDK will return an error if custom_partition_key_type is "measure" and any value is specified for
    // enforce_custom_partition_key
    let enforce_custom_partition_key = match custom_partition_key_type {
        Some(timestream_write::types::PartitionKeyType::Dimension) => {
            // enforce_custom_partition_key value (true or false) is required if custom_partition_key_type is PartitionKeyType::Dimension
            match timestream_env_config.enforce_custom_partition_key {
                true => Some(timestream_write::types::PartitionKeyEnforcementLevel::Required),
                false => Some(timestream_write::types::PartitionKeyEnforcementLevel::Optional),
            }
        }
        _ => None,
    };

    // If custom_partition_key_type is "dimension", then custom_partition_key_dimension is required.
    // The SDK will return an error if custom_partition_key_type is "measure" and
    // any value is specified for custom_partition_key_dimension
    let custom_partition_key_dimension = match custom_partition_key_type {
        Some(timestream_write::types::PartitionKeyType::Dimension) => {
            timestream_env_config.custom_partition_key_dimension.clone()
        }
        _ => None,
    };

    let mag_store_retention_period = match timestream_env_config.mag_store_retention_period {
        Some(val) => val,
        None => {
            let err_message = "Failed to retrieve mag_store_retention_period i64 value";
            error!("{}", err_message);
            return Err(anyhow!(err_message));
        }
    };

    let mem_store_retention_period = match timestream_env_config.mem_store_retention_period {
        Some(val) => val,
        None => {
            let err_message = "Failed to retrieve mem_store_retention_period i64 value";
            error!("{}", err_message);
            return Err(anyhow!(err_message));
        }
    };

    let enable_mag_store_writes = timestream_env_config.enable_mag_store_writes;

    Ok(TableConfig {
        mag_store_retention_period,
        mem_store_retention_period,
        enable_mag_store_writes,
        enforce_custom_partition_key,
        custom_partition_key_type,
        custom_partition_key_dimension,
    })
}

/// Ingest records to Timestream in batches of 100 (Max supported Timestream
/// batch size) in parallel.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn ingest_records(
    client: Arc<timestream_write::Client>,
    database_name: Arc<String>,
    table_name: String,
    records: Vec<timestream_write::types::Record>,
) -> Result<(), Error> {
    let mut records_ingested: usize = 0;
    const MAX_TIMESTREAM_BATCH_SIZE: usize = 100;

    // Chunk records in parallel using rayon (par_chunks)
    let records_chunked: Vec<Vec<timestream_write::types::Record>> = records
        .par_chunks(MAX_TIMESTREAM_BATCH_SIZE)
        .map(|sub_records| sub_records.to_vec())
        .collect();

    // Use a semaphore to limit the maximum number of threads used to ingest chunks in parallel
    let ingestion_semaphore = Arc::new(Semaphore::new(NUM_TIMESTREAM_INGEST_THREADS));
    let mut ingestion_futures = FuturesUnordered::new();

    // Ingest chunks in parallel
    for chunk in records_chunked {
        let permit = ingestion_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Failed to get semaphore permit");
        records_ingested += chunk.len();
        let client_clone = Arc::clone(&client);
        let table_name_clone = table_name.clone();
        let database_name_clone = Arc::clone(&database_name).to_string();

        let future = task::spawn(async move {
            let result =
                ingest_record_batch(client_clone, database_name_clone, table_name_clone, chunk)
                    .await;
            drop(permit);
            result
        });

        ingestion_futures.push(future);
    }

    while let Some(result) = ingestion_futures.next().await {
        // result will be Result<Result<(), Error>>
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                return Err(anyhow!(error));
            }
            Err(error) => {
                return Err(anyhow!(error));
            }
        }
    }

    info!(
        "{} records ingested total for table {} in database {}",
        records_ingested, table_name, database_name
    );

    Ok(())
}

/// Ingests a batch of Timestream records.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn ingest_record_batch(
    client: Arc<timestream_write::Client>,
    database_name: String,
    table_name: String,
    chunk: Vec<timestream_write::types::Record>,
) -> Result<(), Error> {
    match client
        .write_records()
        .database_name(database_name)
        .table_name(table_name)
        .set_records(Some(chunk))
        .send()
        .await
    {
        Ok(_) => {}
        Err(error) => {
            info!("SdkError: {:?}", error.raw_response().unwrap());
            return Err(anyhow!(error));
        }
    };

    Ok(())
}

#[cfg(test)]
#[test]
pub fn test_parse_tags_from_str_empty_string() -> Result<(), Error> {
    let tags_str = "";
    let tags = parse_tags_from_str(tags_str)?;
    assert!(tags.is_empty());
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_whitespace_only() -> Result<(), Error> {
    let tags_str = "   ";
    let tags = parse_tags_from_str(tags_str)?;
    assert!(tags.is_empty());
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_tag_with_no_value() -> Result<(), Error> {
    let tags_str = "key1";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_multiple_tags() -> Result<(), Error> {
    let tags_str = "key1=value1, key2=value2, key3=value3";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "value1");
    assert_eq!(tags[1].key, "key2");
    assert_eq!(tags[1].value, "value2");
    assert_eq!(tags[2].key, "key3");
    assert_eq!(tags[2].value, "value3");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_extra_commas() -> Result<(), Error> {
    let tags_str = "key1=value1, , key2=value2,";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "value1");
    assert_eq!(tags[1].key, "key2");
    assert_eq!(tags[1].value, "value2");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_multiple_equals() -> Result<(), Error> {
    let tags_str = "key1=value=with=equals";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "value=with=equals");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_trimming_whitespace() -> Result<(), Error> {
    let tags_str = "  key1  =  value1  ,   key2=   value2  ";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "value1");
    assert_eq!(tags[1].key, "key2");
    assert_eq!(tags[1].value, "value2");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_tag_with_empty_value_explicit() -> Result<(), Error> {
    let tags_str = "key1=,key2=2,key3";
    let tags = parse_tags_from_str(tags_str)?;
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0].key, "key1");
    assert_eq!(tags[0].value, "");
    assert_eq!(tags[1].key, "key2");
    assert_eq!(tags[1].value, "2");
    assert_eq!(tags[2].key, "key3");
    assert_eq!(tags[2].value, "");
    Ok(())
}

#[test]
pub fn test_parse_tags_from_str_error_empty_key() {
    let tags_str = "=value";
    let err = parse_tags_from_str(tags_str).unwrap_err();
    assert!(err.to_string().contains("Tag key must not be empty"));
}

#[test]
pub fn test_parse_tags_from_str_error_only_equals() {
    let tags_str = "   =   ";
    let err = parse_tags_from_str(tags_str).unwrap_err();
    assert!(err.to_string().contains("Tag key must not be empty"));
}
