use aws_sdk_timestreamwrite::{self as timestream_write};

async fn get_connection() -> Result<timestream_write::Client, timestream_write::Error> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region("us-east-1")
        .load()
        .await;
    let (client, reload) = timestream_write::Client::new(&config)
        .with_endpoint_discovery_enabled()
        .await
        .expect("Failed to get connection to Timestream");
    tokio::task::spawn(reload.reload_task());
    Ok(client)
}

#[allow(dead_code)]
async fn create_database() -> Result<(), timestream_write::Error> {
    let client = get_connection().await?;

    client
        .create_database()
        .set_database_name(Some(String::from("sample-rust-app-devops")))
        .send()
        .await?;

    Ok(())
}

#[allow(dead_code)]
async fn delete_s3_bucket(bucket_name: &str) -> Result<(), aws_sdk_s3::Error> {
    println!("Deleting s3 bucket {:?}", bucket_name);
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region("us-east-1")
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
async fn main() {
    let client = get_connection()
        .await
        .expect("Failed to get connection to Timestream");

    match client
        .describe_table()
        .database_name("tmp-rust-db")
        .table_name("tmp-rust-table")
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
                                match delete_s3_bucket(bucket_name).await {
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
        }

        _ => match client
            .delete_table()
            .database_name("tmp-rust-db")
            .table_name("tmp-rust-table")
            .send()
            .await
        {
            Ok(_) => println!(
                "successfully deleted table {:?}",
                String::from("tmp-rust-table")
            ),
            Err(err) => println!(
                "Failed to delete table {:?}, err: {:?}",
                String::from("tmp-rust-table"),
                err
            ),
        },
    }
    println!("Describing database");
    match client
        .describe_database()
        .database_name("tmp-rust-db")
        .send()
        .await
    {
        Ok(_) => match client
            .delete_database()
            .database_name("tmp-rust-db")
            .send()
            .await
        {
            Ok(_) => println!(
                "Successfully deleted database {:?}",
                String::from("tmp-rust-db")
            ),
            Err(err) => println!(
                "Failed to delete database {:?}, err: {:?}",
                String::from("tmp-rust-db"),
                err
            ),
        },
        Err(_) => {
            println!(
                "Error describing database {:?}, skipping deletion",
                String::from("tmp-rust-db")
            );
        }
    }
}
