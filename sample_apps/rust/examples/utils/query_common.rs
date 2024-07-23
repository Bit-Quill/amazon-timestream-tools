use aws_sdk_timestreamquery as timestream_query;
use aws_sdk_timestreamquery::types;
use aws_types::region::Region;
use clap::Parser;
use std::error::Error;
use std::fs;
use std::io::Write;

static DEFAULT_DATABASE_NAME: &str = "devops_multi_sample_application";
static DEFAULT_OUTPUT_FILE: &str = "query_results.log";
static DEFAULT_REGION: &str = "us-east-1";
static DEFAULT_TABLE_NAME: &str = "host_metrics_sample_application";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    // The Timestream for LiveAnalytics database name to use for all queries
    #[arg(short, long, default_value = DEFAULT_DATABASE_NAME)]
    pub database_name: String,

    // The full name of the output file to write query results to.
    // For example, query_results.log
    #[arg(short, long, default_value = DEFAULT_OUTPUT_FILE)]
    pub output_file: String,

    // The name of the AWS region to use for queries
    #[arg(short, long, default_value = DEFAULT_REGION)]
    pub region: String,

    // The Timestream for LiveAnalytics table name to use for all queries
    #[arg(short, long, default_value = DEFAULT_TABLE_NAME)]
    pub table_name: String,
}

pub async fn get_connection(
    region: &String,
) -> Result<timestream_query::Client, timestream_query::Error> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .load()
        .await;
    let (client, reload) = timestream_query::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failure");
    tokio::task::spawn(reload.reload_task());
    Ok(client)
}

pub fn write(mut file: &fs::File, s: String) -> Result<(), Box<dyn Error>> {
    let s_formatted = format!("{}\n", s);
    let write_result = file.write(s_formatted.as_bytes());
    match write_result {
        Ok(_) => {}

        Err(_) => {
            return Err(String::from("Failed to write to file").into());
        }
    }
    let flush_result = file.flush();
    match flush_result {
        Ok(_) => {}

        Err(_) => {
            return Err(String::from("Failed to flush file").into());
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub fn process_scalar_type(data: &types::Datum) -> Result<String, Box<dyn Error>> {
    data.scalar_value
        .clone()
        .ok_or("Scalar value is None".to_string().into())
}

#[allow(dead_code)]
pub fn process_time_series_type(
    data: &[types::TimeSeriesDataPoint],
    column_info: &types::ColumnInfo,
) -> Result<String, Box<dyn Error>> {
    let mut value = String::new();
    for (i, datum) in data.iter().enumerate() {
        value.push_str(&datum.time);
        value.push(':');

        let column_type = column_info.r#type();
        let column_type_ref = column_type.as_ref().ok_or("Column type is None")?;
        let scalar_type = column_type_ref.scalar_type.to_owned();
        let scalar_type_ref = scalar_type.ok_or("Scalar type is None")?;

        let datum_value = datum.value.as_ref().ok_or("Datum value is None")?;
        let _scalar_value = datum_value
            .scalar_value
            .to_owned()
            .ok_or("Scalar value is None")?;

        if scalar_type_ref.as_str() == "" {
            value.push_str(&process_scalar_type(datum_value)?);
        } else if let Some(array_column_info) = &column_type_ref.array_column_info {
            let array_value = datum_value
                .array_value
                .as_ref()
                .ok_or("Array value is None")?;
            value.push_str(&process_array_type(array_value, array_column_info)?);
        } else if let Some(row_column_info) = &column_type_ref.row_column_info {
            let row_value = datum_value.row_value.as_ref().ok_or("Row value is None")?;
            value.push_str(&process_row_type(&row_value.data, row_column_info)?);
        } else {
            panic!("Bad data type");
        }

        if i != data.len() - 1 {
            value.push_str(", ");
        }
    }
    Ok(value)
}

#[allow(dead_code)]
pub fn process_array_type(
    datum_list: &[types::Datum],
    column_info: &types::ColumnInfo,
) -> Result<String, Box<dyn Error>> {
    let mut value = String::new();
    for (i, datum) in datum_list.iter().enumerate() {
        let column_type = column_info.r#type();
        let column_type_ref = column_type.as_ref().ok_or("Column type is None")?;

        let scalar_type = column_type_ref.scalar_type.to_owned();
        let scalar_type_ref = scalar_type.ok_or("Scalar type is None")?;

        if scalar_type_ref.as_str() != "" {
            value.push_str(&process_scalar_type(datum)?);
        } else if let Some(time_series_measure_value_column_info) =
            &column_type_ref.time_series_measure_value_column_info
        {
            let time_series_value = datum
                .time_series_value
                .as_ref()
                .ok_or("Time series value is None")?;
            value.push_str(&process_time_series_type(
                time_series_value,
                time_series_measure_value_column_info,
            )?);
        } else if let Some(array_column_info) = &column_type_ref.array_column_info {
            let array_value = datum.array_value.as_ref().ok_or("Array value is None")?;
            value.push('[');
            value.push_str(&process_array_type(array_value, array_column_info)?);
            value.push(']');
        } else if let Some(row_column_info) = &column_type_ref.row_column_info {
            let row_value = datum.row_value.as_ref().ok_or("Row value is None")?;
            value.push('[');
            value.push_str(&process_row_type(&row_value.data, row_column_info)?);
            value.push(']');
        } else {
            panic!("Bad column type");
        }

        if i != datum_list.len() - 1 {
            value.push_str(", ");
        }
    }
    Ok(value)
}

#[allow(dead_code)]
pub fn process_row_type(
    data: &[types::Datum],
    metadata: &[types::ColumnInfo],
) -> Result<String, Box<dyn Error>> {
    let mut value = String::new();
    for (i, datum) in data.iter().enumerate() {
        let column_info = metadata[i].clone();
        let column_type = column_info.r#type();
        let column_type_ref = column_type.as_ref().ok_or("Column type is None")?;
        let scalar_type = column_type_ref.scalar_type.to_owned();
        let scalar_type_ref = scalar_type.ok_or("Scalar type is None")?;

        if scalar_type_ref.as_str() != "" {
            // process simple data types
            value.push_str(&process_scalar_type(datum)?);
        } else if let Some(time_series_measure_value_column_info) =
            &column_type_ref.time_series_measure_value_column_info
        {
            let datapoint_list = datum
                .time_series_value
                .as_ref()
                .ok_or("Time series value is None")?;
            value.push('[');
            value.push_str(&process_time_series_type(
                datapoint_list,
                time_series_measure_value_column_info,
            )?);
            value.push(']');
        } else if let Some(array_column_info) = &column_type_ref.array_column_info {
            let array_value = datum.array_value.as_ref().ok_or("Array value is None")?;
            value.push('[');
            value.push_str(&process_array_type(array_value, array_column_info)?);
            value.push(']');
        } else if let Some(row_column_info) = &column_type_ref.row_column_info {
            let row_value = datum.row_value.as_ref().ok_or("Row value is None")?;
            value.push('[');
            value.push_str(&process_row_type(&row_value.data, row_column_info)?);
            value.push(']');
        } else {
            panic!("Bad column type");
        }

        // Comma-seperated column values
        if i != data.len() - 1 {
            value.push_str(", ");
        }
    }
    Ok(value)
}

pub async fn run_query(
    query: String,
    client: &timestream_query::Client,
    f: &std::fs::File,
    max_rows: i32,
) -> Result<(), Box<dyn Error>> {
    let query_client = client.query().clone();

    let mut query_result = query_client
        .clone()
        .max_rows(max_rows)
        .query_string(&query)
        .send()
        .await;

    let mut token: String;
    let mut num_rows = 0;
    loop {
        match query_result {
            Ok(query_success) => {
                num_rows += query_success.rows.len();
                if let Some(new_next_token) = query_success.next_token {
                    // Set token to paginate through results
                    token = new_next_token;
                    query_result = query_client
                        .clone()
                        .max_rows(max_rows)
                        .query_string(&query)
                        .next_token(token)
                        .send()
                        .await;
                } else {
                    break;
                }
            }

            Err(error) => {
                let error_string = error.to_string();
                let message = format!(
                    "Error while querying the query {} : {}",
                    &query, error_string
                );
                println!("{}", message);
                let _ = write(f, message);
                return Err(error_string.into());
            }
        }
    }
    let message = format!("Number of rows: {}", num_rows).to_string();
    println!("{}", message);
    write(f, message)?;
    Ok(())
}
