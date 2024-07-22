use aws_sdk_timestreamwrite as timestream_write;
use chrono::NaiveDateTime;
use csv::Reader;
use std::{error::Error, str::FromStr};
mod utils;
use crate::utils::timestream_helper;
use clap::Parser;

async fn ingest_data(args: &timestream_helper::Args) -> Result<(), Box<dyn Error>> {
    let client = timestream_helper::get_connection(&args.region).await?;

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
                .database_name(&args.database_name)
                .table_name(&args.table_name)
                .set_records(Some(std::mem::take(&mut records)))
                .send()
                .await?;
        }
    }

    if !records.is_empty() {
        client
            .write_records()
            .database_name(&args.database_name)
            .table_name(&args.table_name)
            .set_records(Some(records))
            .send()
            .await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = timestream_helper::Args::parse();

    let client = timestream_helper::get_connection(&args.region)
        .await
        .expect("Failed to get connection to Timestream");

    if let Err(describe_db_error) = client
        .describe_database()
        .database_name(&args.database_name)
        .send()
        .await
    {
        if describe_db_error
            .as_service_error()
            .map(|e| e.is_resource_not_found_exception())
            == Some(true)
        {
            timestream_helper::create_database(&args).await?;
        } else {
            panic!(
                "Failed to describe the database {:?}, Error: {:?}",
                args.database_name, describe_db_error
            );
        }
    }

    if let Err(describe_table_error) = client
        .describe_table()
        .database_name(&args.database_name)
        .table_name(&args.table_name)
        .send()
        .await
    {
        if describe_table_error
            .as_service_error()
            .map(|e| e.is_resource_not_found_exception())
            == Some(true)
        {
            timestream_helper::create_table(&args).await?;
        } else {
            panic!(
                "Failed to describe the table {:?}, Error: {:?}",
                args.table_name, describe_table_error
            );
        }
    }

    ingest_data(&args).await?;
    Ok(())
}
