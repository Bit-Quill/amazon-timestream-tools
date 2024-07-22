use aws_sdk_timestreamwrite as timestream_write;
use aws_types::region::Region;
use clap::Parser;

static DEFAULT_DATABASE_NAME: &str = "devops_multi_sample_application";
static DEFAULT_REGION: &str = "us-east-1";
static DEFAULT_TABLE_NAME: &str = "host_metrics_sample_application";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    // The Timestream for LiveAnalytics database name to use for all queries
    #[arg(short, long, default_value = DEFAULT_DATABASE_NAME)]
    pub database_name: String,

    // The name of the AWS region to use for queries
    #[arg(short, long, default_value = DEFAULT_REGION)]
    pub region: String,

    // The Timestream for LiveAnalytics table name to use for all queries
    #[arg(short, long, default_value = DEFAULT_TABLE_NAME)]
    pub table_name: String,
}

pub async fn get_connection(
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

pub async fn create_database(args: &Args) -> Result<(), timestream_write::Error> {
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

pub async fn create_table(args: &Args) -> Result<(), timestream_write::Error> {
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

pub async fn delete_s3_bucket(bucket_name: &str, region: &String) -> Result<(), aws_sdk_s3::Error> {
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
