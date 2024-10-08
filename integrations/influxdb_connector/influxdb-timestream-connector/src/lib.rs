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

pub fn get_precision(event: &Value) -> Option<&str> {
    // Retrieves the optional "precision" query string parameter from a serde_json::Value

    // Query string parameters may be included as "queryStringParameters"
    if let Some(precision) = event
        .get("queryStringParameters")
        .or_else(|| event.get("queryParameters"))
        .and_then(|query_string_parameters| query_string_parameters.get("precision"))
    {
        // event["queryStringParameters"]["precision"] may be an object
        if let Some(precision_str) = precision.as_str() {
            return Some(precision_str);
        // event["queryStringParameters"]["precision"] may be an array. This is common from requests
        // originating from AWS services, such as when the connector is ran with the cargo lambda watch command
        } else if let Some(precision_array) = precision.as_array() {
            if let Some(precision_value) = precision_array.first().and_then(|value| value.as_str())
            {
                return Some(precision_value);
            }
        }
    }

    None
}

pub async fn lambda_handler(
    client: &timestream_write::Client,
    event: LambdaEvent<Value>,
) -> Result<Value, Error> {
    // Handler for lambda runtime

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
        // This is the format required for custom Lambda responses
        // https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
        Ok(_) => Ok(json!({
            "statusCode": 200,
            "body": "{\"message\": \"Success\"}",
            "isBase64Encoded": false,
            "headers": {
                "Content-Type": "application/json"
            }
        })),
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
