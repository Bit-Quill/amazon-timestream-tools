use aws_sdk_timestreamwrite as timestream_write;

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
async fn delete_s3_bucket(bucket_name: &str) -> Result<(), aws_sdk_s3::Error> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region("us-east-1")
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);

    //    println!(
    //        "Deleting S3 {:?} associated with table {:?}",
    //        bucket_name,
    //        String::from("TODO update me")
    //    );

    //    let the_bucket = client
    //        .get_object()
    //        .set_bucket(Some(bucket_name.to_owned()))
    //        //        .set_key(Some(String::from("timestream")))
    //        .send()
    //        .await;
    let bucket_objects = client
        .list_objects_v2()
        .set_bucket(Some(bucket_name.to_owned()))
        .send()
        .await?
        .contents();
    //    println!("objects: {:?}", bucket_objects);
    //    println!(
    //        "Contents: {:?}",
    //        bucket_objects
    //            .contents()
    //            .get(0)
    //            .expect("Failed to unwrap objects")
    //            .key()
    //            .expect("Failed to unwrap objects key")
    //    );
    for object in bucket_objects {
        client
            .delete_object()
            .set_bucket(Some(bucket_name.to_owned()))
            .set_key(Some(object.key().unwrap().to_owned()))
            .send()
            .await
            .expect("Failed to delete object");
    }

    //    let del = client
    //        .delete_bucket()
    //        .set_bucket(Some(bucket_name.to_owned()))
    //        .send()
    //        .await;

    //println!("bucket metadata {:?}", del);
    Ok(())
}

#[tokio::main]
async fn main() {
    let client = get_connection().await.expect("Failed to get connection");

    // println!("Describing table with name TODO update me");
    let describe_table_output = client
        .describe_table()
        .database_name("tmp-rust-db")
        .table_name("tmp-rust-table")
        .send()
        .await
        .expect("Failed to describe table"); // TODO: Handle non-existent table gracefully

    match describe_table_output.table() {
        Some(table) => match table.magnetic_store_write_properties() {
            Some(mag_store) => match mag_store.enable_magnetic_store_writes() {
                true => match mag_store.magnetic_store_rejected_data_location() {
                    Some(rejected_data_locale) => match rejected_data_locale.s3_configuration() {
                        Some(s3_config) => {
                            let _ = delete_s3_bucket(
                                s3_config.bucket_name().expect("Failed to unwrap string"),
                            )
                            .await;
                        }
                        _ => (),
                    },
                    _ => println!("Everything else"),
                },
                _ => println!("third 3"),
            },
            _ => println!("second 2"),
        },
        _ => println!("first 1"),
    };
    let _ = describe_table_output.table.unwrap();
}
