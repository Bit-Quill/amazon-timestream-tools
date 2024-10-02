use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use lambda_runtime::LambdaEvent;
use line_protocol_parser::*;
use records_builder::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::{str, thread, time};
use timestream_utils::*;

pub mod line_protocol_parser;
pub mod metric;
pub mod records_builder;
pub mod timestream_utils;

// The maximum number of database/table creation/delete API calls
// that can be made per second is 1.
pub static TIMESTREAM_API_WAIT_SECONDS: u64 = 1;

async fn handle_body(
    client: &timestream_write::Client,
    body: &[u8],
    precision: &timestream_write::types::TimeUnit,
) -> Result<(), Error> {
    // Handle parsing body in request

    let line_protocol = str::from_utf8(body).unwrap();
    let metric_data = parse_line_protocol(line_protocol)?;
    let multi_measure_builder = get_builder(SchemaType::MultiTableMultiMeasure(std::env::var(
        "measure_name_for_multi_measure_records",
    )?));

    // Only currently supports multi-measure multi-table
    let multi_table_batch = build_records(&multi_measure_builder, &metric_data, precision)?;
    handle_multi_table_ingestion(client, multi_table_batch).await?;
    Ok(())
}

async fn handle_multi_table_ingestion(
    client: &timestream_write::Client,
    records: HashMap<String, Vec<timestream_write::types::Record>>,
) -> Result<(), Error> {
    // Ingestion for multi-measure schema type

    let database_name = std::env::var("database_name")?;

    match database_exists(client, &database_name).await {
        Ok(true) => (),
        Ok(false) => {
            if database_creation_enabled()? {
                thread::sleep(time::Duration::from_secs(TIMESTREAM_API_WAIT_SECONDS));
                create_database(client, &database_name).await?;
            } else {
                return Err(anyhow!(
                    "Database {} does not exist and database creation is not enabled",
                    database_name
                ));
            }
        }
        Err(error) => return Err(anyhow!(error)),
    }

    for (table_name, _) in records.iter() {
        match table_exists(client, &database_name, table_name).await {
            Ok(true) => (),
            Ok(false) => {
                if table_creation_enabled()? {
                    thread::sleep(time::Duration::from_secs(TIMESTREAM_API_WAIT_SECONDS));
                    create_table(client, &database_name, table_name, get_table_config()?).await?
                } else {
                    return Err(anyhow!(
                        "Table {} does not exist and database creation is not enabled",
                        table_name
                    ));
                }
            }
            Err(error) => println!("error checking table exists: {:?}", error),
        }
    }

    for (table_name, mut records) in records.into_iter() {
        ingest_records(client, &database_name, &table_name, &mut records).await?
    }

    Ok(())
}

pub async fn lambda_handler(
    client: &timestream_write::Client,
    event: LambdaEvent<Value>,
) -> Result<Value, Error> {
    // Handler for lambda runtime
    let (event, _context) = event.into_parts();

    // Get the "precision" query parameter, if included.
    // Query parameters can be included with different spellings.
    // AWS services use "queryParameters", the InfluxDB Go client
    // uses "queryStringParameters"
    let precision: timestream_write::types::TimeUnit = event
        .as_object()
        .and_then(|map| {
            map.keys()
                .find(|key| {
                    let key_lower = key.to_lowercase();
                    key_lower.starts_with("query") && key_lower.ends_with("parameters")
                })
                .and_then(|query_parameters_key| map.get(query_parameters_key))
        })
        .and_then(|parameters| {
            parameters
                .get("precision")
                .and_then(|user_precision| user_precision.as_str())
        })
        .map_or(
            timestream_write::types::TimeUnit::Nanoseconds,
            |precision_str| match precision_str {
                "ms" => timestream_write::types::TimeUnit::Milliseconds,
                "us" => timestream_write::types::TimeUnit::Microseconds,
                "s" => timestream_write::types::TimeUnit::Seconds,
                _ => timestream_write::types::TimeUnit::Nanoseconds,
            },
        );

    let data = event
        .get("body")
        .expect("No body was included in the request")
        .as_str()
        .expect("Failed to convert body to &str")
        .as_bytes();

    match handle_body(client, data, &precision).await {
        Ok(_) => Ok(json!({
            "statusCode": 200,
            "body": "{\"message\": \"Success\"}",
            "isBase64Encoded": false,
            "headers": {
                "Content-Type": "application/json"
            }
        })),
        Err(error) => Err(anyhow!(error.to_string())),
    }
}
