use crate::metric::Metric;
use anyhow::{anyhow, Error};
use aws_sdk_timestreamwrite as timestream_write;
use log::trace;
use std::{collections::HashMap, time::Instant};

mod multi_table_multi_measure_builder;

const DIMENSION_PARTITION_KEY_TYPE: &str = "dimension";
const MEASURE_PARTITION_KEY_TYPE: &str = "measure";

pub enum SchemaType {
    MultiTableMultiMeasure(String),
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SchemaType::MultiTableMultiMeasure(v) => v.fmt(f),
        }
    }
}

pub fn get_builder(schema: SchemaType) -> impl BuildRecords {
    // Currently only supported schema is multi-table multi-measure
    let function_start = Instant::now();
    let build_records_impl = multi_table_multi_measure_builder::MultiTableMultiMeasureBuilder {
        measure_name: schema.to_string(),
    };
    trace!("get_builder duration: {:?}", function_start.elapsed());
    build_records_impl
}

pub fn build_records(
    records_builder: &impl BuildRecords,
    metrics: &[Metric],
    precision: &timestream_write::types::TimeUnit,
) -> Result<HashMap<String, Vec<timestream_write::types::Record>>, Error> {
    let function_start = Instant::now();
    let result = records_builder.build_records(metrics, precision);
    trace!("build_records duration: {:?}", function_start.elapsed());
    result
}

pub struct TableConfig {
    pub mag_store_retention_period: i64,
    pub mem_store_retention_period: i64,
    pub enable_mag_store_writes: bool,
    pub enforce_custom_partition_key: Option<timestream_write::types::PartitionKeyEnforcementLevel>,
    pub custom_partition_key_type: Option<timestream_write::types::PartitionKeyType>,
    pub custom_partition_key_dimension: Option<String>,
}

pub fn get_table_config() -> Result<TableConfig, Error> {
    // Get the populated table_config struct

    let function_start = Instant::now();

    let custom_partition_key_type = match std::env::var("custom_partition_key_type") {
        Ok(custom_partition_key_type_value) => {
            match custom_partition_key_type_value.to_lowercase().as_str() {
                DIMENSION_PARTITION_KEY_TYPE => {
                    Some(timestream_write::types::PartitionKeyType::Dimension)
                }
                MEASURE_PARTITION_KEY_TYPE => {
                    Some(timestream_write::types::PartitionKeyType::Measure)
                }
                _ => None,
            }
        }
        _ => None,
    };

    // If custom_partition_key_type is "dimension", then enforce_custom_partition_key is required (true or false).
    // If custom_partition_key_type is "measure", then this will ignore enforce_custom_partition_key.
    // The SDK will return an error if custom_partition_key_type is "measure" and any value is specified for
    // enforce_custom_partition_key
    let enforce_custom_partition_key = match custom_partition_key_type {
        Some(timestream_write::types::PartitionKeyType::Dimension) => {
            // enforce_custom_partition_key value (true or false) is required if custom_partition_key_type is PartitionKeyType::Dimension
            match std::env::var("enforce_custom_partition_key")?
                .to_lowercase()
                .as_str()
            {
                "true" | "t" | "1" => {
                    Some(timestream_write::types::PartitionKeyEnforcementLevel::Required)
                }
                "false" | "f" | "0" => {
                    Some(timestream_write::types::PartitionKeyEnforcementLevel::Optional)
                }
                _ => None,
            }
        }
        _ => None,
    };

    // If custom_partition_key_type is "dimension", then custom_partition_key_dimension is required.
    // The SDK will return an error if custom_partition_key_type is "measure" and
    // any value is specified for custom_partition_key_dimension
    let custom_partition_key_dimension = match custom_partition_key_type {
        Some(timestream_write::types::PartitionKeyType::Dimension) => {
            Some(std::env::var("custom_partition_key_dimension")?)
        }
        _ => None,
    };

    let config = Ok(TableConfig {
        mag_store_retention_period: std::env::var("mag_store_retention_period")?.parse()?,
        mem_store_retention_period: std::env::var("mem_store_retention_period")?.parse()?,
        enable_mag_store_writes: matches!(
            std::env::var("enable_mag_store_writes")?
                .to_lowercase()
                .as_str(),
            "true" | "t" | "1"
        ),
        enforce_custom_partition_key,
        custom_partition_key_type,
        custom_partition_key_dimension,
    });

    trace!("get_table_config duration: {:?}", function_start.elapsed());
    config
}

pub fn table_creation_enabled() -> Result<bool, Error> {
    // Convert the env var table_creation_enabled to bool

    let function_start = Instant::now();
    match std::env::var("enable_table_creation") {
        Ok(enabled) => {
            let result = Ok(env_var_to_bool(enabled));
            trace!(
                "table_creation_enabled duration: {:?}",
                function_start.elapsed()
            );
            result
        }
        Err(_) => Err(anyhow!(
            "enable_table_creation environment variable is not defined"
        )),
    }
}

pub fn database_creation_enabled() -> Result<bool, Error> {
    // Convert the env var database_creation_enabled to bool

    let function_start = Instant::now();
    match std::env::var("enable_database_creation") {
        Ok(enabled) => {
            let result = Ok(env_var_to_bool(enabled));
            trace!(
                "database_creation_enabled duration: {:?}",
                function_start.elapsed()
            );
            result
        }
        Err(_) => Err(anyhow!(
            "enable_database_creation environment variable is not defined"
        )),
    }
}

pub fn env_var_to_bool(env_var: String) -> bool {
    // Convert the env var to bool

    let function_start = Instant::now();
    let val = matches!(env_var.as_str(), "true" | "t" | "1");
    trace!("env_var_to_bool duration: {:?}", function_start.elapsed());
    val
}

pub fn validate_env_variables() -> Result<(), Error> {
    // Validate environment variables for all schema types

    let function_start = Instant::now();

    if std::env::var("region").is_err() {
        return Err(anyhow!("region environment variable is not defined"));
    }
    if std::env::var("database_name").is_err() {
        return Err(anyhow!("database_name environment variable is not defined"));
    }
    if std::env::var("enable_database_creation").is_err() {
        return Err(anyhow!(
            "enable_database_creation environment variable is not defined"
        ));
    }
    let enable_table_creation = std::env::var("enable_table_creation");

    if enable_table_creation.is_err() {
        return Err(anyhow!(
            "enable_table_creation environment variable is not defined"
        ));
    }

    if env_var_to_bool(enable_table_creation?) {
        if std::env::var("enable_mag_store_writes").is_err() {
            return Err(anyhow!(
                "enable_mag_store_writes environment variable is not defined"
            ));
        }
        if std::env::var("mag_store_retention_period").is_err() {
            return Err(anyhow!(
                "mag_store_retention_period environment variable is not defined"
            ));
        }
        if std::env::var("mem_store_retention_period").is_err() {
            return Err(anyhow!(
                "mem_store_retention_period environment variable is not defined"
            ));
        }
    }

    // Customer-defined partition key environment variables
    let custom_partition_key_type = std::env::var("custom_partition_key_type");

    if let Ok(custom_partition_key_type) = custom_partition_key_type {
        if custom_partition_key_type != DIMENSION_PARTITION_KEY_TYPE
            && custom_partition_key_type != MEASURE_PARTITION_KEY_TYPE
        {
            return Err(anyhow!(
                format!("custom_partition_key_type can only be {DIMENSION_PARTITION_KEY_TYPE} or {MEASURE_PARTITION_KEY_TYPE}")
            ));
        }

        // Check required environment variables for when custom partition key type is "dimension." If it is "measure,"
        // no other environment variables are necessary.

        let custom_partition_key_dimension = std::env::var("custom_partition_key_dimension");

        if custom_partition_key_type == DIMENSION_PARTITION_KEY_TYPE
            && custom_partition_key_dimension.is_err()
        {
            return Err(anyhow!(
                format!("If custom_partition_key_type is {DIMENSION_PARTITION_KEY_TYPE}, then custom_partition_key_dimension must be defined")
            ));
        }

        let enforce_custom_partition_key = std::env::var("enforce_custom_partition_key");

        if custom_partition_key_type == DIMENSION_PARTITION_KEY_TYPE
            && enforce_custom_partition_key.is_err()
        {
            return Err(anyhow!(
                format!("enforce_custom_partition_key value must be specified (true or false) when custom_partition_key_type is {DIMENSION_PARTITION_KEY_TYPE}")
            ));
        }
    }

    trace!(
        "validate_env_variables duration: {:?}",
        function_start.elapsed()
    );
    Ok(())
}

pub trait BuildRecords {
    fn build_records(
        &self,
        metrics: &[Metric],
        precision: &timestream_write::types::TimeUnit,
    ) -> Result<HashMap<String, Vec<timestream_write::types::Record>>, Error>;
}

#[cfg(test)]
pub mod tests;
