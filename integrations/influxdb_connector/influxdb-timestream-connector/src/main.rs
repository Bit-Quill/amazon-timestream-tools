use influxdb_timestream_connector::{
    lambda_handler, records_builder::validate_env_variables, timestream_utils::get_connection,
};
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Error> {
    validate_env_variables()?;
    let region = std::env::var("region")?;
    let timestream_client = get_connection(&region).await?;
    tracing::init_default_subscriber();
    run(service_fn(|event: LambdaEvent<Value>| {
        lambda_handler(&timestream_client, event)
    }))
    .await
}
