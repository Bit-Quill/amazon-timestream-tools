use anyhow::{anyhow, Error, Result};
use aws_sdk_timestreamwrite as timestream_write;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use lambda_runtime::LambdaEvent;
use line_protocol_parser::*;
use log::{info, trace};
use records_builder::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use std::{str, thread, time};
use timestream_utils::*;
use tokio::sync::Semaphore;
use tokio::task;

pub mod line_protocol_parser;
pub mod metric;
pub mod records_builder;
pub mod timestream_utils;

// The maximum number of database/table creation/delete API calls
// that can be made per second is 1.
pub static TIMESTREAM_API_WAIT_SECONDS: u64 = 1;

// The number of batches processed at the same time.
// For multi-table multi measure schema, batches are a combination of
// a table name and a Vec of records bound for that table
pub static NUM_BATCH_THREADS: usize = 16;

async fn handle_body(
    client: &Arc<timestream_write::Client>,
    body: &[u8],
    precision: &timestream_write::types::TimeUnit,
) -> Result<(), Error> {
    // Handle parsing body in request

    let function_start = Instant::now();

    let line_protocol = str::from_utf8(body).unwrap();
    let metric_data = parse_line_protocol(line_protocol)?;
    let multi_measure_builder = get_builder(SchemaType::MultiTableMultiMeasure(std::env::var(
        "measure_name_for_multi_measure_records",
    )?));

    // Only currently supports multi-measure multi-table
    let multi_table_batch = build_records(&multi_measure_builder, &metric_data, precision)?;
    handle_multi_table_ingestion(client, multi_table_batch).await?;
    trace!("handle_body duration: {:?}", function_start.elapsed());
    Ok(())
}

async fn handle_multi_table_ingestion(
    client: &Arc<timestream_write::Client>,
    records: HashMap<String, Vec<timestream_write::types::Record>>,
) -> Result<(), Error> {
    // Ingestion for multi-measure schema type

    let function_start = Instant::now();

    let database_name = std::env::var("database_name")?;
    let database_name = Arc::new(database_name);

    if let Ok(true) = std::env::var("enable_database_creation").map(env_var_to_bool) {
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
    }

    // Use a semaphore to limit the maximum number of threads used to process batches in parallel
    let semaphore = Arc::new(Semaphore::new(NUM_BATCH_THREADS));
    let mut batch_ingestion_futures = FuturesUnordered::new();

    // Track total time taken to check existence of tables and ingest records
    let ingestion_start = Instant::now();

    // Ingest records for each table, in parallel
    for (table_name, records) in records {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Failed to get semaphore permit");

        // Use Arc::clone to create a shallow clone of the client
        let client_clone = Arc::clone(client);
        let database_name_clone = Arc::clone(&database_name);

        // Create a future for ingesting to the current table
        let future = task::spawn(async move {
            // Check whether the table exists
            if let Ok(true) = std::env::var("enable_table_creation").map(env_var_to_bool) {
                match table_exists(&client_clone, &database_name_clone, &table_name).await {
                    Ok(true) => (),
                    Ok(false) => {
                        if table_creation_enabled()? {
                            thread::sleep(time::Duration::from_secs(TIMESTREAM_API_WAIT_SECONDS));
                            create_table(
                                &client_clone,
                                &database_name_clone,
                                &table_name,
                                get_table_config()?,
                            )
                            .await?
                        } else {
                            return Err(anyhow!(
                                "Table {} does not exist and database creation is not enabled",
                                table_name
                            ));
                        }
                    }
                    Err(error) => info!("error checking table exists: {:?}", error),
                }
            }

            // Ingest the data to the table
            let result =
                ingest_records(client_clone, database_name_clone, table_name, records).await;
            drop(permit);
            result
        });
        batch_ingestion_futures.push(future);
    }

    while let Some(result) = batch_ingestion_futures.next().await {
        // result will be Result<Result<(), Error>>
        // This means the nested Result needs to be checked
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

    trace!(
        "Total asynchronous ingestion duration: {:?}",
        ingestion_start.elapsed()
    );
    trace!(
        "handle_multi_table_ingestion duration: {:?}",
        function_start.elapsed()
    );
    Ok(())
}

pub fn get_precision(event: &Value) -> Option<&str> {
    // Retrieves the optional "precision" query string parameter from a serde_json::Value

    let function_start = Instant::now();

    // Query string parameters may be included as "queryStringParameters"
    if let Some(precision) = event
        .get("queryStringParameters")
        .or_else(|| event.get("queryParameters"))
        .and_then(|query_string_parameters| query_string_parameters.get("precision"))
    {
        // event["queryStringParameters"]["precision"] may be an object
        if let Some(precision_str) = precision.as_str() {
            trace!("get_precision duration: {:?}", function_start.elapsed());
            return Some(precision_str);
        // event["queryStringParameters"]["precision"] may be an array. This is common from requests
        // originating from AWS services, such as when the connector is ran with the cargo lambda watch command
        } else if let Some(precision_array) = precision.as_array() {
            if let Some(precision_value) = precision_array.first().and_then(|value| value.as_str())
            {
                trace!("get_precision duration: {:?}", function_start.elapsed());
                return Some(precision_value);
            }
        }
    }

    None
}

pub async fn lambda_handler(
    client: &Arc<timestream_write::Client>,
    event: LambdaEvent<Value>,
) -> Result<Value, Error> {
    // Handler for lambda runtime

    let function_start = Instant::now();

    let (event, _context) = event.into_parts();

    let precision = match get_precision(&event) {
        Some("ms") => timestream_write::types::TimeUnit::Milliseconds,
        Some("us") => timestream_write::types::TimeUnit::Microseconds,
        Some("s") => timestream_write::types::TimeUnit::Seconds,
        _ => timestream_write::types::TimeUnit::Nanoseconds,
    };

    let data = event
        .get("body")
        .expect("No body was included in the request")
        .as_str()
        .expect("Failed to convert body to &str")
        .as_bytes();

    match handle_body(client, data, &precision).await {
        // This is the format required for custom Lambda 1.0 responses
        // https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
        Ok(_) => {
            let mut response = json!({
                "statusCode": 200,
                "body": "{\"message\": \"Success\"}",
                "isBase64Encoded": false,
                "headers": {
                    "Content-Type": "application/json"
                }
            });
            // cargo lambda watch expects a Lambda response in 2.0 format.
            // This means a "cookies" array must be added to the response.
            // If this "cookies" array is present and the connector is deployed
            // with synchronous invocation in a stack, users will receive a
            // 502 error
            if std::env::var("local_invocation").is_ok() {
                response["cookies"] = json!([]);
            }
            trace!("lambda_handler duration: {:?}", function_start.elapsed());
            Ok(response)
        }
        // An Err is required in order to send messages to the Lambda's
        // dead letter queue, when the connector is deployed as part of a stack
        // with asynchronous invocation
        Err(error) => Err(anyhow!(error.to_string())),
    }
}

#[cfg(test)]
#[test]
pub fn test_get_precision_query_string_parameters_array() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": ["ms"] } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ms");
    Ok(())
}

#[test]
pub fn test_get_precision_query_string_parameters_object() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": "ms" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ms");
    Ok(())
}

#[test]
pub fn test_get_precision_query_string_parameters_object_nanoseconds() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": "ns" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ns");
    Ok(())
}

#[test]
pub fn test_get_precision_query_string_parameters_object_microseconds() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": "us" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "us");
    Ok(())
}

#[test]
pub fn test_get_precision_query_string_parameters_object_seconds() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "precision": "s" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "s");
    Ok(())
}

#[test]
pub fn test_get_precision_query_parameters_array() -> Result<(), Error> {
    let fake_event_value = json!({ "queryParameters": { "precision": ["ms"] } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ms");
    Ok(())
}

#[test]
pub fn test_get_precision_query_parameters_object() -> Result<(), Error> {
    let fake_event_value = json!({ "queryParameters": { "precision": "ms" } });
    let precision = get_precision(&fake_event_value);
    assert!(precision.is_some());
    assert!(precision.expect("Failed to get precision") == "ms");
    Ok(())
}

#[test]
pub fn test_get_precision_incorrect_query_parameters_key() -> Result<(), Error> {
    let fake_event_value = json!({ "nomatch": { "precision": "ms" } });
    assert!(get_precision(&fake_event_value).is_none());
    Ok(())
}

#[test]
pub fn test_get_precision_incorrect_precision_key() -> Result<(), Error> {
    let fake_event_value = json!({ "queryStringParameters": { "nomatch": "ms" } });
    assert!(get_precision(&fake_event_value).is_none());
    Ok(())
}
