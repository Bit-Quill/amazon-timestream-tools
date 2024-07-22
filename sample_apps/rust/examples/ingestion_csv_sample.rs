use aws_sdk_timestreamwrite as timestream_write;
use aws_types::region::Region;
use chrono::NaiveDateTime;
use clap::Parser;
use csv::Reader;
use std::{error::Error, str::FromStr};

static DEFAULT_DATABASE_NAME: &str = "devops_multi_sample_application";
static DEFAULT_REGION: &str = "us-east-1";
static DEFAULT_TABLE_NAME: &str = "host_metrics_sample_application";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    // The Timestream for LiveAnalytics database name to use for all queries
    #[arg(short, long, default_value = DEFAULT_DATABASE_NAME)]
    database_name: String,

    // The name of the AWS region to use for queries
    #[arg(short, long, default_value = DEFAULT_REGION)]
    region: String,

    // The Timestream for LiveAnalytics table name to use for all queries
    #[arg(short, long, default_value = DEFAULT_TABLE_NAME)]
    table_name: String,
}

async fn get_connection(
    region: &String,
) -> Result<timestream_write::Client, timestream_write::Error> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .load()
        .await;
    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failure");
    tokio::task::spawn(reload.reload_task());
    Ok(client)
}

async fn ingest_data(args: &Args) -> Result<(), Box<dyn Error>> {
    let client = get_connection(&args.region).await.unwrap();

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

async fn create_database(args: &Args) -> Result<(), timestream_write::Error> {
    let client = get_connection(&args.region)
        .await
        .expect("Failed to get connection");

    client
        .create_database()
        .set_database_name(Some(args.database_name.to_owned()))
        .send()
        .await?;

    Ok(())
}

async fn create_table(args: &Args) -> Result<(), timestream_write::Error> {
    let client = get_connection(&args.region)
        .await
        .expect("Failed to get connection");

    client
        .create_table()
        .set_table_name(Some(args.table_name.to_owned()))
        .set_database_name(Some(args.database_name.to_owned()))
        .send()
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let client = get_connection(&args.region)
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
            create_database(&args).await?;
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
            create_table(&args).await?;
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
