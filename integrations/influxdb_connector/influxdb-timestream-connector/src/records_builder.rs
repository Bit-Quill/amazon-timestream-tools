use crate::metric::Metric;
use anyhow::Error;
use aws_sdk_timestreamwrite::types as timestream_types;
use std::{collections::HashMap, fmt::Debug};

mod multi_measure_builder;

#[derive(Debug)]
pub enum SchemaType {
    MultiTableMultiMeasure,
    SingleTableMultiMeasure,
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
) -> Result<HashMap<String, Vec<timestream_types::Record>>, Error> {
    records_builder.build_records(metrics, precision)
}

pub trait BuildRecords: Debug {
    fn build_records(
        &self,
        metrics: &[Metric],
        precision: &timestream_types::TimeUnit,
    ) -> Result<HashMap<String, Vec<timestream_types::Record>>, Error>;
}

#[cfg(test)]
pub mod tests;
