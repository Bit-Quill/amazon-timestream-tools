use super::records_builder::TableConfig;
use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use aws_types::region::Region;
use std::io::Write;

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
    client: &timestream_write::Client,
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
    client: &timestream_write::Client,
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

    client
        .create_table()
        .set_table_name(Some(table_name.to_owned()))
        .set_database_name(Some(database_name.to_owned()))
        .set_retention_properties(Some(retention_properties))
        .set_magnetic_store_write_properties(Some(magnetic_store_properties))
        .send()
        .await?;

    Ok(())
}

pub async fn table_exists(
    client: &timestream_write::Client,
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
    client: &timestream_write::Client,
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
    client: &timestream_write::Client,
    database_name: &str,
    table_name: &str,
    records: &mut [timestream_write::types::Record],
) -> Result<(), Error> {
    // Ingest records to Timestream in batches of 100 (Max supported Timestream batch size)

    let mut records_ingested: usize = 0;
    const MAX_TIMESTREAM_BATCH_SIZE: usize = 100;

    let mut records_chunked: Vec<Vec<timestream_write::types::Record>> = records
        .chunks(MAX_TIMESTREAM_BATCH_SIZE)
        .map(|sub_records| sub_records.to_vec())
        .collect();
    for chunk in records_chunked.iter_mut() {
        records_ingested += chunk.len();
        match client
            .write_records()
            .database_name(database_name)
            .table_name(table_name)
            .set_records(Some(std::mem::take(chunk)))
            .send()
            .await
        {
            Ok(_) => {
                println!("{} records ingested", records_ingested);
                std::io::stdout().flush()?;
            }
            Err(error) => {
                println!("SdkError: {:?}", error.raw_response().unwrap());
                return Err(anyhow!(error));
            }
        }
    }
    println!(
        "{} records ingested total for table {} in database {}",
        records_ingested, table_name, database_name
    );

    Ok(())
}