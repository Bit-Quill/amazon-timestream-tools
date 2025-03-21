use crate::metric::Metric;
use anyhow::Error;
use async_trait::async_trait;
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
pub async fn build_records(
    records_builder: &impl BuildRecords,
    metrics: &[Metric],
    precision: &timestream_types::TimeUnit,
) -> Result<TableGroupedRecords, Error> {
    records_builder.build_records(metrics, precision).await
}

#[async_trait]
pub trait BuildRecords: Debug {
    async fn build_records(
        &self,
        metrics: &[Metric],
        precision: &timestream_types::TimeUnit,
    ) -> Result<TableGroupedRecords, Error>;
}

#[cfg(test)]
pub mod tests;
