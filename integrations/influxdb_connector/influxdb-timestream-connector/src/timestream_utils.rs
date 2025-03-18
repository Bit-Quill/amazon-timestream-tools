use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use aws_types::region::Region;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{error, info};
use rand::Rng;
use rayon::prelude::{ParallelIterator, ParallelSlice};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task;

/// The maximum number of threads to use for ingesting
/// batches of records to Timestream in parallel.
static NUM_TIMESTREAM_INGEST_THREADS: usize = 12;

/// The maximum number of database/table creation/delete API calls
/// that can be made per second is 1.
pub static TIMESTREAM_API_BASE_WAIT_SECONDS: u64 = 1;

/// The maximum number of retries for any Timestream API call.
static MAX_RETRIES: u32 = 7;

pub const DIMENSION_PARTITION_KEY_TYPE: &str = "dimension";
pub const MEASURE_PARTITION_KEY_TYPE: &str = "measure";

#[derive(Debug)]
pub struct TableConfig {
    pub mag_store_retention_period: i64,
    pub mem_store_retention_period: i64,
    pub enable_mag_store_writes: bool,
    pub enforce_custom_partition_key: Option<timestream_write::types::PartitionKeyEnforcementLevel>,
    pub custom_partition_key_type: Option<timestream_write::types::PartitionKeyType>,
    pub custom_partition_key_dimension: Option<String>,
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
) -> Result<(), Error> {
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

    let create_db_result = retry_with_backoff(|| create_db_builder.clone().send()).await;

    match create_db_result {
        Ok(_) => {
            info!("Database '{}' created successfully.", database_name);
            Ok(())
        }
        Err(err) => {
            let err_message = format!("Failed to create database '{}': {:?}", database_name, err);
            error!("{}", err_message);
            Err(anyhow!(err_message))
        }
    }
}

/// Create a new Timestream table.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn create_table(
    client: &Arc<timestream_write::Client>,
    database_name: &str,
    table_name: &str,
    table_config: TableConfig,
    tags: Option<Vec<timestream_write::types::Tag>>,
) -> Result<(), Error> {
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

    let create_table_result = retry_with_backoff(|| create_table_builder.clone().send()).await;

    match create_table_result {
        Ok(_) => {
            info!(
                "Table '{}' created successfully in database '{}'.",
                table_name, database_name
            );
            return Ok(());
        }
        Err(err) => {
            let err_message = format!(
                "Failed to create table '{}' in database '{}': {:?}",
                table_name, database_name, err
            );
            error!("{}", err_message);
            return Err(anyhow!(err_message));
        }
    }
}

/// Checks if a table already exists.
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

/// Checks if a database already exists.
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

/// Retries a Timestream operation with exponential backoff and jitter.
/// The operation F must return a Future where the output of the Future is a Result<T, E>,
/// where T is any successful output, and E is an error that can be translated
/// into an anyhow::Error. This is tailored for Timestream Write API calls.
///
/// # Examples
///
/// ```
/// // Create a Timestream table.
/// // The builder must be cloned as each call to send() consumes the builder.
/// retry_with_backoff(|| create_table_builder.clone().send()).await?;
/// ```
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub async fn retry_with_backoff<F, Fut, T, E>(mut operation: F) -> Result<(), Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: Into<Error>,
{
    for attempt in 0..MAX_RETRIES {
        match operation().await {
            Ok(_) => {
                return Ok(());
            }
            Err(err) => {
                if attempt == MAX_RETRIES - 1 {
                    return Err(anyhow!(err));
                }

                let exp_backoff =
                    Duration::from_secs(TIMESTREAM_API_BASE_WAIT_SECONDS * u64::pow(2, attempt));
                let jitter = Duration::from_millis(rand::thread_rng().gen_range(0, 1000));
                let delay = exp_backoff + jitter;

                info!(
                    "Attempt {}/{} failed. Retrying in {}s",
                    attempt + 1,
                    MAX_RETRIES,
                    delay.as_secs()
                );

                thread::sleep(delay);
            }
        }
    }
    unreachable!()
}

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

/// Gets a populated table_config struct.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn get_table_config() -> Result<TableConfig, Error> {
    let custom_partition_key_type = match std::env::var("custom_partition_key_type") {
        Ok(custom_partition_key_type_value) => {
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
            match std::env::var("enforce_custom_partition_key")?
                .to_lowercase()
                .as_str()
            {
                "true" | "t" | "1" => {
                    Some(timestream_write::types::PartitionKeyEnforcementLevel::Required)
                }
                "false" | "f" | "0" => {
                    Some(timestream_write::types::PartitionKeyEnforcementLevel::Optional)
                }
                _ => None,
            }
        }
        _ => None,
    };

    // If custom_partition_key_type is "dimension", then custom_partition_key_dimension is required.
    // The SDK will return an error if custom_partition_key_type is "measure" and
    // any value is specified for custom_partition_key_dimension
    let custom_partition_key_dimension = match custom_partition_key_type {
        Some(timestream_write::types::PartitionKeyType::Dimension) => {
            Some(std::env::var("custom_partition_key_dimension")?)
        }
        _ => None,
    };

    Ok(TableConfig {
        mag_store_retention_period: std::env::var("mag_store_retention_period")?.parse()?,
        mem_store_retention_period: std::env::var("mem_store_retention_period")?.parse()?,
        enable_mag_store_writes: matches!(
            std::env::var("enable_mag_store_writes")?
                .to_lowercase()
                .as_str(),
            "true" | "t" | "1"
        ),
        enforce_custom_partition_key,
        custom_partition_key_type,
        custom_partition_key_dimension,
    })
}

/// Ingests records to Timestream in batches of 100 (max supported Timestream batch size)
/// in parallel.
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
            error!("SdkError: {:?}", error.raw_response().unwrap());
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
