use aws_sdk_timestreamwrite::{self as timestream_write};
use aws_types::region::Region;
use clap::Parser;
use std::error::Error;

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

async fn delete_s3_bucket(bucket_name: &str, region: &String) -> Result<(), aws_sdk_s3::Error> {
    println!("Deleting s3 bucket {:?}", bucket_name);
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_owned()))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);

    for object in client
        .list_objects_v2()
        .set_bucket(Some(bucket_name.to_owned()))
        .send()
        .await?
        .contents()
    {
        client
            .delete_object()
            .set_bucket(Some(bucket_name.to_owned()))
            .set_key(Some(object.key().unwrap().to_owned()))
            .send()
            .await?;
    }

    let mut object_versions = client
        .list_object_versions()
        .set_bucket(Some(bucket_name.to_owned()))
        .send()
        .await?;

    loop {
        for delete_marker in object_versions.delete_markers() {
            client
                .delete_object()
                .set_bucket(Some(bucket_name.to_owned()))
                .set_key(Some(delete_marker.key().unwrap().to_owned()))
                .send()
                .await?;
        }

        for version in object_versions.versions() {
            client
                .delete_object()
                .set_bucket(Some(bucket_name.to_owned()))
                .set_key(Some(version.key().unwrap().to_owned()))
                .send()
                .await?;
        }

        if object_versions.is_truncated().unwrap() {
            object_versions = client
                .list_object_versions()
                .set_bucket(Some(bucket_name.to_owned()))
                .key_marker(object_versions.next_key_marker().unwrap())
                .version_id_marker(object_versions.next_version_id_marker().unwrap())
                .send()
                .await?;
        } else {
            break;
        }
    }

    let _ = client
        .delete_bucket()
        .set_bucket(Some(bucket_name.to_owned()))
        .send()
        .await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let client = get_connection(&args.region)
        .await
        .expect("Failed to get connection to Timestream");

    match client
        .describe_table()
        .database_name(&args.database_name)
        .table_name(&args.table_name)
        .send()
        .await
    {
        Ok(describe_table_output) => {
            if let Some(table) = describe_table_output.table() {
                if let Some(magnetic_store) = table.magnetic_store_write_properties() {
                    if magnetic_store.enable_magnetic_store_writes() {
                        if let Some(rejected_data_location) =
                            magnetic_store.magnetic_store_rejected_data_location()
                        {
                            if let Some(s3_config) = rejected_data_location.s3_configuration() {
                                let bucket_name = s3_config
                                    .bucket_name()
                                    .expect("Failed to retrieve bucket name");
                                match delete_s3_bucket(bucket_name, &args.region).await {
                                    Ok(_) => {
                                        println!("s3 bucket {:?} successfully deleted", bucket_name)
                                    }
                                    Err(err) => println!(
                                        "Failed to delete s3 bucket {:?}, err: {:?}",
                                        bucket_name, err
                                    ),
                                }
                            }
                        }
                    }
                }
            }
            client
                .delete_table()
                .database_name(&args.database_name)
                .table_name(&args.table_name)
                .send()
                .await?;
        }

        Err(describe_table_error) => {
            if describe_table_error
                .as_service_error()
                .map(|e| e.is_resource_not_found_exception())
                == Some(true)
            {
                println!(
                    "Skipping table deletion as the table {:?} does not exist",
                    args.table_name
                );
            } else {
                panic!(
                    "Failed to describe the table {:?}, Error: {:?}",
                    args.table_name, describe_table_error
                );
            }
        }
    }

    match client
        .describe_database()
        .database_name(&args.database_name)
        .send()
        .await
    {
        Ok(_) => match client
            .delete_database()
            .database_name(&args.database_name)
            .send()
            .await
        {
            Ok(_) => println!("Successfully deleted database {:?}", &args.database_name),
            Err(err) => println!(
                "Failed to delete database {:?}, err: {:?}",
                &args.database_name, err
            ),
        },
        Err(describe_database_error) => {
            if describe_database_error
                .as_service_error()
                .map(|e| e.is_resource_not_found_exception())
                == Some(true)
            {
                println!(
                    "Skipping database deletion as the database {:?} does not exist",
                    args.database_name
                );
            } else {
                panic!(
                    "Failed to describe the database {:?}, Error: {:?}",
                    args.database_name, describe_database_error
                );
            }
        }
    }

    Ok(())
}
