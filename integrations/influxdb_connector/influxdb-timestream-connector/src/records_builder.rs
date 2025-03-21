use crate::metric::Metric;
use crate::timestream_utils::DIMENSION_PARTITION_KEY_TYPE;
use anyhow::{anyhow, Error};
use aws_sdk_timestreamwrite::types as timestream_types;
use std::{collections::HashMap, fmt::Debug};

mod multi_measure_builder;

#[derive(Debug)]
pub enum SchemaType {
    MultiTableMultiMeasure,
    SingleTableMultiMeasure,
}

/// A group of records sharing common attributes.
#[derive(Debug, Clone)]
pub struct AttributeGroupedRecords {
    /// A Record comprised of a measure name, dimensions, and timestamp precision.
    /// All records within the "records" field share these attributes.
    pub common_attributes: timestream_types::Record,
    /// Records containing measure values and timestamps. These records share
    /// attributes contained in the "common_attributes" field.
    pub records: Vec<timestream_types::Record>,
}

impl AttributeGroupedRecords {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// A single Record split into its common attributes and measure value(s).
#[derive(Debug, Clone)]
pub struct RecordPair {
    /// A Record comprised of a measure name, dimensions, and timestamp precision.
    /// The Record in the "record" field shares these attributes.
    common_attributes: timestream_types::Record,
    /// A Record containing measure value(s) and a timestamp.
    record: timestream_types::Record,
}

/// A group of records grouped by a table name.
#[derive(Debug, Clone)]
pub struct TableGroupedRecords {
    table_groups: HashMap<String, HashMap<String, AttributeGroupedRecords>>,
}

impl TableGroupedRecords {
    pub fn new() -> Self {
        Self {
            table_groups: HashMap::new(),
        }
    }

    /// Inserts a record pair into the "table_groups" field. The record pair will be
    /// grouped with other records with the same common attributes to be ingested
    /// into the same table.
    pub fn insert_record_pair(&mut self, table_name: String, record_pair: RecordPair) {
        let attributes_key = format!("{:?}", record_pair.common_attributes);

        if let Some(attributes_map) = self.table_groups.get_mut(table_name.as_str()) {
            if let Some(attribute_grouped_records) = attributes_map.get_mut(attributes_key.as_str())
            {
                // Insert record into existing group of records
                attribute_grouped_records.records.push(record_pair.record);
            } else {
                // Create a new AttributeGroupedRecords, as this is the
                // first entry for this set of common attributes
                attributes_map.insert(
                    attributes_key,
                    AttributeGroupedRecords {
                        common_attributes: record_pair.common_attributes,
                        records: vec![record_pair.record],
                    },
                );
            }
        // Inner HashMap does not exist, create it
        } else {
            self.table_groups.insert(
                table_name.to_string(),
                HashMap::from([(
                    attributes_key,
                    AttributeGroupedRecords {
                        common_attributes: record_pair.common_attributes,
                        records: vec![record_pair.record],
                    },
                )]),
            );
        }
    }

    pub fn num_tables(self) -> usize {
        self.table_groups.len()
    }

    pub fn num_common_attributes_grouped_records(self) -> usize {
        let mut num: usize = 0;
        for (_, map) in self.table_groups {
            num += map.len();
        }
        num
    }

    pub fn len(&self) -> usize {
        self.table_groups.len()
    }

    /// Returns the total number of Records.
    pub fn num_records(&self) -> usize {
        self.table_groups
            .values()
            .map(|common_attributes_grouped_records_map| {
                common_attributes_grouped_records_map
                    .values()
                    .map(|common_attributes_grouped_records| {
                        common_attributes_grouped_records.records.len()
                    })
                    .sum::<usize>()
            })
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.table_groups.is_empty()
    }

    pub fn get(&mut self, table_name: &str) -> Option<&HashMap<String, AttributeGroupedRecords>> {
        self.table_groups.get(table_name)
    }

    /// Gets a Vec of AttributeGroupedRecords. If a table name is provided,
    /// all AttributeGroupedRecords are for that table. Otherwise, all
    /// groups of AttributeGroupedRecords are returned.
    pub fn get_attribute_grouped_records_vec(
        &self,
        table_name: Option<&str>,
    ) -> Vec<&AttributeGroupedRecords> {
        match table_name {
            Some(name) => self
                .table_groups
                .get(name)
                .map(|common_attributes_grouped_records_map| {
                    common_attributes_grouped_records_map.values().collect()
                })
                .unwrap_or_default(), // Return an empty Vec
            None => self
                .table_groups
                .values()
                .flat_map(|common_attributes_grouped_records_map| {
                    common_attributes_grouped_records_map.values()
                })
                .collect(),
        }
    }
}

impl IntoIterator for TableGroupedRecords {
    type Item = (String, HashMap<String, AttributeGroupedRecords>);
    type IntoIter =
        std::collections::hash_map::IntoIter<String, HashMap<String, AttributeGroupedRecords>>;

    fn into_iter(self) -> Self::IntoIter {
        self.table_groups.into_iter()
    }
}

impl Default for TableGroupedRecords {
    fn default() -> Self {
        Self::new()
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn get_builder(schema: SchemaType, measure_name: String) -> impl BuildRecords {
    match schema {
        SchemaType::SingleTableMultiMeasure => {
            return multi_measure_builder::MultiMeasureBuilder {
                measure_name: None,
                schema_type: SchemaType::SingleTableMultiMeasure,
            }
        }
        SchemaType::MultiTableMultiMeasure => {
            return multi_measure_builder::MultiMeasureBuilder {
                measure_name: Some(measure_name.to_string()),
                schema_type: SchemaType::MultiTableMultiMeasure,
            }
        }
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn build_records(
    records_builder: &impl BuildRecords,
    metrics: &[Metric],
    precision: &timestream_types::TimeUnit,
) -> Result<TableGroupedRecords, Error> {
    records_builder.build_records(metrics, precision)
}

/// Converts the environment variable "table_creation_enabled" to bool.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn table_creation_enabled() -> Result<bool, Error> {
    match std::env::var("enable_table_creation") {
        Ok(enabled) => Ok(env_var_to_bool(enabled)),
        Err(_) => Err(anyhow!(
            "enable_table_creation environment variable is not defined"
        )),
    }
}

/// Converts the environment variable "database_creation_enabled" to bool.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn database_creation_enabled() -> Result<bool, Error> {
    match std::env::var("enable_database_creation") {
        Ok(enabled) => Ok(env_var_to_bool(enabled)),
        Err(_) => Err(anyhow!(
            "enable_database_creation environment variable is not defined"
        )),
    }
}

/// Converts an environment variable to bool.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn env_var_to_bool(env_var: String) -> bool {
    matches!(env_var.as_str(), "true" | "t" | "1")
}

/// Validates environment variables for all schema types.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn validate_env_variables() -> Result<(), Error> {
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

    // Validate environment variables for table mapping
    match std::env::var("table_mapping") {
        Ok(table_mapping) => match table_mapping.as_str() {
            "single-table" => {
                if std::env::var("single_table_name").is_err() {
                    return Err(anyhow!(
                        "single_table_name environment variable is not defined"
                    ));
                }
            }
            "multi-table" => {
                if std::env::var("measure_name_for_multi_measure_records").is_err() {
                    return Err(anyhow!(
                        "measure_name_for_multi_measure_records environment variable is not defined"
                    ));
                }
            }
            table_mapping => {
                return Err(anyhow!(
                    "{:?} is an invalid value for the table_mapping environment variable",
                    table_mapping
                ))
            }
        },
        Err(_) => return Err(anyhow!("table_mapping environment variable is not defined")),
    }

    // Customer-defined partition key environment variables
    let custom_partition_key_type = std::env::var("custom_partition_key_type");

    if let Ok(custom_partition_key_type) = custom_partition_key_type {
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

    Ok(())
}

pub trait BuildRecords: Debug {
    fn build_records(
        &self,
        metrics: &[Metric],
        precision: &timestream_types::TimeUnit,
    ) -> Result<TableGroupedRecords, Error>;
}

#[cfg(test)]
pub mod tests;
