use super::records_builder::TableConfig;
use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use aws_types::region::Region;
use futures::future::join_all;
use rayon::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use tokio::task;

pub async fn get_connection(
    region: &str,
) -> Result<timestream_write::Client, timestream_write::Error> {
    // Get a connection to Timestream

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .load()
        .await;
    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failed to get the write client connection with Timestream");

    tokio::task::spawn(reload.reload_task());
    println!("Initialized connection to Timestream in region {}", region);
    Ok(client)
}

pub async fn create_database(
    client: &Arc<timestream_write::Client>,
    database_name: &str,
) -> Result<(), timestream_write::Error> {
    // Create a new Timestream database

    println!("Creating new database {}", database_name);
    client
        .create_database()
        .set_database_name(Some(database_name.to_owned()))
        .send()
        .await?;

    Ok(())
}

pub async fn create_table(
    client: &Arc<timestream_write::Client>,
    database_name: &str,
    table_name: &str,
    table_config: TableConfig,
) -> Result<(), timestream_write::Error> {
    // Create a new Timestream table

    println!(
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

    client
        .create_table()
        .set_schema(table_schema)
        .set_table_name(Some(table_name.to_owned()))
        .set_database_name(Some(database_name.to_owned()))
        .set_retention_properties(Some(retention_properties))
        .set_magnetic_store_write_properties(Some(magnetic_store_properties))
        .send()
        .await?;

    Ok(())
}

pub async fn table_exists(
    client: &Arc<timestream_write::Client>,
    database_name: &str,
    table_name: &str,
) -> Result<bool, Error> {
    // Check if table already exists

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

pub async fn database_exists(
    client: &Arc<timestream_write::Client>,
    database_name: &str,
) -> Result<bool, Error> {
    // Check if database already exists

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

pub async fn ingest_records(
    client: Arc<timestream_write::Client>,
    database_name: String,
    table_name: String,
    records: Vec<timestream_write::types::Record>,
) -> Result<(), Error> {
    // Ingest records to Timestream in batches of 100 (Max supported Timestream batch size)
    let start = Instant::now();

    let mut records_ingested: usize = 0;
    const MAX_TIMESTREAM_BATCH_SIZE: usize = 100;

    let records_chunked: Vec<Vec<timestream_write::types::Record>> = records
        .par_chunks(MAX_TIMESTREAM_BATCH_SIZE)
        .map(|sub_records| sub_records.to_vec())
        .collect();
    let mut tasks = Vec::new();
    for chunk in records_chunked {
        records_ingested += chunk.len();
        let client_clone = Arc::clone(&client);
        let task = task::spawn(ingest_record_batch(
            client_clone,
            database_name.to_string(),
            table_name.to_string(),
            chunk,
        ));
        tasks.push(task);
    }
    let _result = join_all(tasks).await;

    println!(
        "{} records ingested total for table {} in database {}",
        records_ingested, table_name, database_name
    );

    let duration = start.elapsed();
    println!("Time elapsed: {:?}", duration);

    Ok(())
}

pub async fn ingest_record_batch(
    client: Arc<timestream_write::Client>,
    database_name: String,
    table_name: String,
    chunk: Vec<timestream_write::types::Record>,
) -> Result<(), Error> {
    // Ingest records to Timestream in batches of 100 (Max supported Timestream batch size)
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
            println!("SdkError: {:?}", error.raw_response().unwrap());
            return Err(anyhow!(error));
        }
    };

    Ok(())
}
