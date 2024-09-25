use crate::metric::Metric;
use anyhow::{anyhow, Error};
use aws_sdk_timestreamwrite as timestream_write;
use std::collections::HashMap;

mod multi_table_multi_measure_builder;

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
    multi_table_multi_measure_builder::MultiTableMultiMeasureBuilder {
        measure_name: schema.to_string(),
    }
}

pub fn build_records(
    records_builder: &impl BuildRecords,
    metrics: &[Metric],
    precision: &timestream_write::types::TimeUnit,
) -> Result<HashMap<String, Vec<timestream_write::types::Record>>, Error> {
    records_builder.build_records(metrics, precision)
}

pub struct TableConfig {
    pub mag_store_retention_period: i64,
    pub mem_store_retention_period: i64,
    pub enable_mag_store_writes: bool,
}

pub fn get_table_config() -> Result<TableConfig, Error> {
    // Get the populated table_config struct

    Ok(TableConfig {
        mag_store_retention_period: std::env::var("mag_store_retention_period")?.parse()?,
        mem_store_retention_period: std::env::var("mem_store_retention_period")?.parse()?,
        enable_mag_store_writes: matches!(
            std::env::var("enable_mag_store_writes")?
                .to_lowercase()
                .as_str(),
            "true" | "t" | "1"
        ),
    })
}

pub fn table_creation_enabled() -> Result<bool, Error> {
    // Convert the env var table_creation_enabled to bool

    match std::env::var("enable_table_creation") {
        Ok(enabled) => Ok(env_var_to_bool(enabled)),
        Err(_) => Err(anyhow!(
            "enable_table_creation environment variable is not defined"
        )),
    }
}

pub fn database_creation_enabled() -> Result<bool, Error> {
    // Convert the env var database_creation_enabled to bool

    match std::env::var("enable_database_creation") {
        Ok(enabled) => Ok(env_var_to_bool(enabled)),
        Err(_) => Err(anyhow!(
            "enable_database_creation environment variable is not defined"
        )),
    }
}

pub fn env_var_to_bool(env_var: String) -> bool {
    // Convert the env var to bool

    matches!(env_var.as_str(), "true" | "t" | "1")
}

pub fn validate_env_variables() -> Result<(), Error> {
    // Validate environment variables for all schema types

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
