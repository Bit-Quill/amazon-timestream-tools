use aws_sdk_timestreamwrite as timestream_write;
use chrono::NaiveDateTime;
use csv::Reader;
use std::{error::Error, str::FromStr};

async fn get_connection() -> Result<timestream_write::Client, timestream_write::Error> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region("us-east-1")
        .load()
        .await;
    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failure");
    tokio::task::spawn(reload.reload_task());
    Ok(client)
}

async fn ingest_data() -> Result<(), Box<dyn Error>> {
    let client = get_connection().await.unwrap();

    let mut records: Vec<timestream_write::types::Record> = Vec::new();
    let mut csv_reader = Reader::from_path("../data/sample.csv")?;
    for record in csv_reader.records() {
        let record_result = record.expect("Failed to read csv record");

        records.push(
            timestream_write::types::Record::builder()
                .set_dimensions(Some(vec![
                    timestream_write::types::Dimension::builder()
                        .name(&record_result[0])
                        .value(&record_result[1])
                        .build()
                        .unwrap(),
                    timestream_write::types::Dimension::builder()
                        .name(&record_result[2])
                        .value(&record_result[3])
                        .build()
                        .unwrap(),
                    timestream_write::types::Dimension::builder()
                        .name(&record_result[4])
                        .value(&record_result[5])
                        .build()
                        .unwrap(),
                ]))
                .measure_name(&record_result[6])
                .measure_value(&record_result[7])
                .measure_value_type(
                    timestream_write::types::MeasureValueType::from_str(&record_result[8])
                        .expect("Failed to enumerate measure value type"),
                )
                .time(
                    NaiveDateTime::parse_from_str(&record_result[9], "%Y-%m-%d %H:%M:%S%.9f")?
                        .and_utc()
                        .timestamp_millis()
                        .to_string(),
                )
                .time_unit(
                    timestream_write::types::TimeUnit::from_str(&record_result[10])
                        .expect("Failed to parse time unit"),
                )
                .build(),
        );

        if records.len() == 100 {
            client
                .write_records()
                .database_name(String::from("sample-rust-app-devops"))
                .table_name(String::from("sample-rust-app-host-metrics"))
                .set_records(Some(std::mem::take(&mut records)))
                .send()
                .await?;
        }
    }

    if !records.is_empty() {
        client
            .write_records()
            .database_name(String::from("sample-rust-app-devops"))
            .table_name(String::from("sample-rust-app-host-metrics"))
            .set_records(Some(records))
            .send()
            .await?;
    }
    Ok(())
}

#[allow(dead_code)]
async fn create_database() -> Result<(), timestream_write::Error> {
    let client = get_connection().await.expect("Failed to get connection");

    let db_tags: Vec<timestream_write::types::Tag> = vec![timestream_write::types::Tag::builder()
        .set_key(Some(String::from("db-tag-key")))
        .set_value(Some(String::from("db-tag-val")))
        .build()
        .expect("Failed to build tag")];

    client
        .create_database()
        .set_tags(Some(db_tags))
        .set_database_name(Some(String::from("sample-rust-app-devops")))
        .send()
        .await?;

    Ok(())
}

#[allow(dead_code)]
async fn create_table() -> Result<(), timestream_write::Error> {
    let client = get_connection().await.expect("Failed to get connection");

    let tbl_tags: Vec<timestream_write::types::Tag> = vec![timestream_write::types::Tag::builder()
        .set_key(Some(String::from("tbl-tag-key")))
        .set_value(Some(String::from("tbl-tag-val")))
        .build()
        .expect("Failed to build tag")];

    // let tbl_schema = timestream_write::types::Schema::builder().set_composite_partition_key()

    client
        .create_table()
        .set_table_name(Some(String::from("sample-rust-app-host-metrics")))
        .set_database_name(Some(String::from("sample-rust-app-devops")))
        .set_tags(Some(tbl_tags))
        .send()
        .await?;

    Ok(())

    //.set_retention_properties()
    //.set_magnetic_store_write_properties()
    //.set_schema(Some(tbl_schema))
}

#[tokio::main]
async fn main() {
    // ingest_data().await;
    // create_database().await;
    // create_table().await;
    let _ = ingest_data().await;
}
