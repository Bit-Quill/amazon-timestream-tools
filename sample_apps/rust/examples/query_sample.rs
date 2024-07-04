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
async fn execute_sample_queries() -> Result<(), timestream_write::Error> {
    let _client = get_connection().await.expect("Failed to get connection");

    println!("Finish My Implementation");

    Ok(())
}

#[allow(dead_code)]
#[tokio::main]
async fn main() {
    let _ = execute_sample_queries().await;
}
