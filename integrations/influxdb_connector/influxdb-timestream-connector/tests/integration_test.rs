use anyhow::Error;
use aws_credential_types::Credentials;
use aws_sdk_timestreamwrite as timestream_write;
use aws_types::region::Region;
use lambda_http::http::StatusCode;
use lambda_http::{http::Request, IntoResponse};
use lambda_http::{Body, RequestExt};
use rand::{distributions::uniform::SampleUniform, distributions::Alphanumeric, Rng};
use std::collections::HashMap;
use std::{env, thread, time};

static DATABASE_NAME: &str = "influxdb_timestream_connector_integ_db";
static REGION: &str = "us-west-2";
static MAX_TIMESTREAM_TABLE_NAME_LENGTH: usize = 256;

// A batch of resources to be deleted at the end of a test.
struct CleanupBatch {
    client: timestream_write::Client,
    database_name: String,
    table_names_to_delete: Vec<String>,
}

impl CleanupBatch {
    pub fn new(
        client: timestream_write::Client,
        database_name: String,
        table_names_to_delete: Vec<String>,
    ) -> CleanupBatch {
        CleanupBatch {
            client,
            database_name,
            table_names_to_delete,
        }
    }

    async fn cleanup(&mut self) {
        for table_name_to_delete in self.table_names_to_delete.iter() {
            println!(
                "Deleting table {} in database {}",
                table_name_to_delete, self.database_name
            );
            thread::sleep(time::Duration::from_secs(
                influxdb_timestream_connector::TIMESTREAM_API_WAIT_SECONDS,
            ));
            let result = self
                .client
                .delete_table()
                .database_name(&self.database_name)
                .table_name(table_name_to_delete)
                .send()
                .await;
            match result {
                Ok(_) => (),

                Err(error) => {
                    println!(
                        "Table deletion failed for table {}: {:?}",
                        table_name_to_delete,
                        error.raw_response()
                    );
                }
            }
        }
    }
}

fn set_environment_variables() {
    env::set_var("database_name", DATABASE_NAME);
    env::set_var("enable_database_creation", "true");
    env::set_var("enable_table_creation", "true");
    env::set_var("enable_mag_store_writes", "true");
    // A value of 30,000 allows single-digit timestamps to be ingested.
    env::set_var("mag_store_retention_period", "30000");
    env::set_var("mem_store_retention_period", "12");
    env::set_var("region", REGION);
    env::set_var(
        "measure_name_for_multi_measure_records",
        "test_measure_name",
    );
}

fn random_string(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

fn random_number<T: PartialOrd + SampleUniform>(low: T, high: T) -> T {
    rand::thread_rng().gen_range(low, high)
}

// These integration tests use the InfluxDB Timestream connector as a library
// instead of deploying the connector as a lambda and then making
// requests to the connector. Each test builds a lambda_http::http::Request and
// passes it to the connector's lambda_handler function. This means integration
// testing has minimal overhead.

#[tokio::test]
async fn test_mtmm_basic() -> Result<(), Error> {
    // Tests ingesting a single point.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_multiple_timestamps() -> Result<(), Error> {
    // Tests ingesting a single point with two timestamps.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {} {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis(),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() != StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn test_mtmm_many_tags_many_fields() -> Result<(), Error> {
    // Tests ingesting a single point with 50 tags and 50 fields.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let mut point = format!("{},", lp_measurement_name);

    for i in 0..50 {
        point.push_str(&format!("tag{}={}", i, random_string(9)));
        if i != 49 {
            point.push(',');
        }
    }
    point.push(' ');
    for i in 0..50 {
        point.push_str(&format!("field{}={}i", i, random_number(0, 100001)));
        if i != 49 {
            point.push(',');
        }
    }
    point.push_str(&format!(" {}\n", chrono::Utc::now().timestamp_millis()));

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");
    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_float() -> Result<(), Error> {
    // Tests ingesting a single point with a float value for the field.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={} {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0.0, 100001.0),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_string() -> Result<(), Error> {
    // Tests ingesting a single point with a string value for the field.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1=\"{}\" {}\n",
        lp_measurement_name,
        random_string(9),
        random_string(9),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_bool() -> Result<(), Error> {
    // Tests ingesting a single point with a bool value for the field.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={} {}\n",
        lp_measurement_name,
        random_string(9),
        true,
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_tag_length() -> Result<(), Error> {
    // Tests ingesting a single point with a tag where the tag's key
    // is 60, the maximum allowed dimension name length, and its value
    // is 1988 characters long. The length of the tag key and tag value
    // together amount to the maximum size for a dimension pair, 2 kilobytes.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},{}={} field1={}i {}\n",
        lp_measurement_name,
        random_string(60),
        random_string(1988),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_tag_length() -> Result<(), Error> {
    // Tests ingesting a single point with a tag where the tag's key
    // is 60, the maximum allowed dimension name length, and its value
    // is 1989 characters long. The length of the tag key and tag value
    // together exceed the maximum size for a dimension pair, 2 kilobytes.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},{}={} field1={}i {}\n",
        lp_measurement_name,
        random_string(60),
        random_string(1989),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() != StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_field_length() -> Result<(), Error> {
    // Tests ingesting a single point with a field where the length
    // of the field key is the maximum measure name, 256, and the length
    // of the field value is the maximum measure value size, 2048.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},field1={} {}=\"{}\" {}\n",
        lp_measurement_name,
        random_string(9),
        random_string(256),
        random_string(2048),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_field_length() -> Result<(), Error> {
    // Tests ingesting a single point with a field where the length
    // of the field key is the maximum measure name, 256, and the length
    // of the field value is beyond the maximum measure value size, 2048.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},field1={} {}=\"{}\" {}\n",
        lp_measurement_name,
        random_string(9),
        random_string(256),
        random_string(2049),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() != StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_unique_field_keys() -> Result<(), Error> {
    // Tests ingesting a batch of points where the number of unique field keys
    // in the batch equals the maximum number of unique measures for a single
    // table, 1024.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..1024 {
        let point = format!(
            "{},tag1={} field{}={}i {}\n",
            lp_measurement_name,
            random_string(9),
            i,
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(lp_batch))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_unique_field_keys() -> Result<(), Error> {
    // Tests ingesting a batch of points where the number of unique field keys
    // in the batch exceeds the maximum number of unique measures for a single
    // table, 1024.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..1025 {
        let point = format!(
            "{},tag1={} field{}={}i {}\n",
            lp_measurement_name,
            random_string(9),
            i,
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(lp_batch))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() != StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_unique_tag_keys() -> Result<(), Error> {
    // Tests ingesting a batch of points where the number of unique tag keys
    // in the batch equals the maximum number of unique dimensions for a single
    // table, 128.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..128 {
        let point = format!(
            "{},tag{}={} field1={}i {}\n",
            lp_measurement_name,
            i,
            random_string(9),
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(lp_batch))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_unique_tag_keys() -> Result<(), Error> {
    // Tests ingesting a batch of points where the number of unique tag keys
    // in the batch exceeds the maximum number of unique dimensions for a single
    // table, 128.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..129 {
        let point = format!(
            "{},tag{}={} field1={}i {}\n",
            lp_measurement_name,
            i,
            random_string(9),
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(lp_batch))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() != StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_max_table_name_length() -> Result<(), Error> {
    // Tests ingesting a single point with measurement with length
    // equal to the maximum number of bytes a Timestream table name can
    // have.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = random_string(MAX_TIMESTREAM_TABLE_NAME_LENGTH);

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_beyond_max_table_name_length() {
    // Tests ingesting a single point with measurement with length
    // exceeding the maximum number of bytes a Timestream table name can
    // have.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = random_string(MAX_TIMESTREAM_TABLE_NAME_LENGTH + 1);

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))
        .expect("Failed to create request")
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await
        .expect("Failed to send request to lambda handler")
        .into_response()
        .await;

    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() != StatusCode::OK);
}

#[tokio::test]
async fn test_mtmm_nanosecond_precision() -> Result<(), Error> {
    // Tests ingesting a single point with nanosecond precision.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now()
            .timestamp_nanos_opt()
            .expect("Failed to create nanosecond timestamp")
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ns".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_microsecond_precision() -> Result<(), Error> {
    // Tests ingesting a single point with microsecond precision.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_micros()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "us".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_second_precision() -> Result<(), Error> {
    // Tests ingesting a single point with second precision.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "s".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_no_precision() -> Result<(), Error> {
    // Tests ingesting a single point without precision supplied.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        // Without a precision provided, the connector should assume the precision
        // is nanoseconds.
        chrono::offset::Utc::now()
            .timestamp_nanos_opt()
            .expect("Failed to create nanosecond timestamp")
    );

    let request = Request::builder()
        .method("POST")
        .body(Body::Text(point))?;

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;

    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_empty_point() -> Result<(), Error> {
    // Tests ingesting an empty string.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let point = String::new();

    let request = Request::builder()
        .method("POST")
        .body(Body::Text(point))?;

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;

    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    Ok(())
}

#[tokio::test]
pub async fn test_mtmm_small_timestamp() -> Result<(), Error> {
    // Tests ingesting with a single-digit millisecond timestamp.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        3
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_5_measurements() -> Result<(), Error> {
    // Tests ingesting a batch with five measurements.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let mut table_names_to_delete = Vec::<String>::new();
    let lp_measurement_name = String::from("readings");

    let mut lp_batch = String::new();
    for i in 0..5 {
        let lp_measurement_name = format!("{lp_measurement_name}{i}").to_string();
        table_names_to_delete.push(lp_measurement_name.clone());

        let point = format!(
            "{},tag1={} field1={}i {}\n",
            lp_measurement_name,
            random_string(9),
            random_number(0, 100001),
            chrono::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(lp_batch))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), table_names_to_delete);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_100_measurements() -> Result<(), Error> {
    // Tests ingesting a batch with 100 measurements.
    set_environment_variables();
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let mut table_names_to_delete = Vec::<String>::new();

    let lp_measurement_name = String::from("readings");
    let mut lp_batch = String::new();
    for i in 0..100 {
        let lp_measurement_name = format!("{lp_measurement_name}{i}").to_string();
        table_names_to_delete.push(lp_measurement_name.clone());

        let point = format!(
            "{},tag1={} field1={}i {}\n",
            lp_measurement_name,
            random_string(9),
            random_number(0, 100001),
            chrono::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(lp_batch))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), table_names_to_delete);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
async fn test_mtmm_5000_batch() -> Result<(), Error> {
    // Tests ingesting a batch of 5000 points with a single measurement.
    // 5000 is the recommended batch size for InfluxDB v2 OSS.
    set_environment_variables();
    // Cleanup
    let client = influxdb_timestream_connector::timestream_utils::get_connection(REGION)
        .await
        .expect("Failed to get client");

    let lp_measurement_name = String::from("readings");
    let mut lp_batch = String::new();

    for _ in 0..5000 {
        let point = format!(
            "{},tag1={} field1={}i {}\n",
            lp_measurement_name,
            random_string(9),
            random_number(0, 100001),
            chrono::offset::Utc::now().timestamp_millis()
        );
        lp_batch.push_str(&point);
    }

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(lp_batch))?
        .with_query_string_parameters(query_parameters);

    let response = influxdb_timestream_connector::lambda_handler(&client, request)
        .await?
        .into_response()
        .await;
    println!("Response: {}: {:?}", response.status(), response.body());
    assert!(response.status() == StatusCode::OK);

    let mut cleanup_batch =
        CleanupBatch::new(client, DATABASE_NAME.to_string(), vec![lp_measurement_name]);
    cleanup_batch.cleanup().await;

    Ok(())
}

#[tokio::test]
#[should_panic]
async fn test_mtmm_no_credentials() {
    // Tests ingesting without AWS credentials. This test should panic.
    set_environment_variables();

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(Credentials::new("", "", None, None, "test"))
        .region(Region::new(REGION))
        .load()
        .await;

    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failed to get the write client connection with Timestream");
    tokio::task::spawn(reload.reload_task());

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))
        .expect("Failed to created request")
        .with_query_string_parameters(query_parameters);

    let _response = influxdb_timestream_connector::lambda_handler(&client, request).await;
}

#[tokio::test]
#[should_panic]
async fn test_mtmm_incorrect_credentials() {
    // Tests ingesting with incorrect AWS credentials. This test should panic.
    set_environment_variables();

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            "ANOTREAL",
            "notrealrnrELgWzOk3IfjzDKtFBhDby",
            None,
            None,
            "test",
        ))
        .region(Region::new(REGION))
        .load()
        .await;

    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failed to get the write client connection with Timestream");
    tokio::task::spawn(reload.reload_task());

    let lp_measurement_name = String::from("readings");

    let point = format!(
        "{},tag1={} field1={}i {}\n",
        lp_measurement_name,
        random_string(9),
        random_number(0, 100001),
        chrono::offset::Utc::now().timestamp_millis()
    );

    let query_parameters = HashMap::from([("precision".to_string(), "ms".to_string())]);
    let request = Request::builder()
        .body(Body::Text(point))
        .expect("Failed to created request")
        .with_query_string_parameters(query_parameters);

    let _response = influxdb_timestream_connector::lambda_handler(&client, request).await;
}
