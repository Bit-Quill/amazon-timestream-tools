use super::{BuildRecords, RecordPair, TableGroupedRecords};
use crate::{
    metric::{FieldValue, Metric},
    timestream_utils::TimestreamEnvConfig,
    SchemaType,
};
use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use aws_sdk_timestreamwrite as timestream_write;

pub struct MultiMeasureBuilder {
    pub measure_name: Option<String>,
    pub schema_type: SchemaType,
}

/// Trait implementation to support multi-measure records Timestream.
#[async_trait]
impl BuildRecords for MultiMeasureBuilder {
    #[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
    async fn build_records(
        &self,
        metrics: &[Metric],
        precision: &timestream_write::types::TimeUnit,
    ) -> Result<TableGroupedRecords, Error> {
        match self.schema_type {
            SchemaType::SingleTableMultiMeasure => {
                let records = build_single_table_multi_measure_records(metrics, precision).await;
                return records;
            }
            SchemaType::MultiTableMultiMeasure => {
                return build_multi_table_multi_measure_records(
                    metrics,
                    self.measure_name.as_deref(),
                    precision,
                )
            }
        }
    }
}

impl std::fmt::Debug for MultiMeasureBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "{}",
            self.measure_name
                .as_deref()
                .expect("Failed to unwrap")
                .to_owned()
        )
    }
}

/// Builds multi-measure records HashMap to be ingested to one table.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
async fn build_single_table_multi_measure_records(
    metrics: &[Metric],
    precision: &timestream_write::types::TimeUnit,
) -> Result<TableGroupedRecords, Error> {
    let timestream_env_config = TimestreamEnvConfig::get().await?;

    let mut records_batch: TableGroupedRecords = TableGroupedRecords::new();
    let table_name = timestream_env_config
        .single_table_name
        .ok_or(anyhow!("Failed to get single_table_name"))?;
    for metric in metrics.iter() {
        let record_pair = metric_to_timestream_record_pair(metric.name(), metric, precision)?;
        records_batch.insert_record_pair(table_name.to_string(), record_pair);
    }

    Ok(records_batch)
}

/// Builds multi-measure records HashMap to be ingested to multiple tables.
/// The HashMap's key is the table name, the inner HashMap groups records
/// according to a common attribute record. The key of this inner HashMap
/// is the records' list of dimensions and measure name as a String. The
/// value for this HashMap, the tuple, is a pair of a common attribute record
/// made up of a list of dimensions and a measure name, and a Vec of records
/// that share that common attribute.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
fn build_multi_table_multi_measure_records(
    metrics: &[Metric],
    measure_name: Option<&str>,
    precision: &timestream_write::types::TimeUnit,
) -> Result<TableGroupedRecords, Error> {
    let mut records_batch: TableGroupedRecords = TableGroupedRecords::new();

    for metric in metrics.iter() {
        let record_pair = metric_to_timestream_record_pair(
            measure_name.expect("Failed to unwrap"),
            metric,
            precision,
        )?;
        let table_name = metric.name();
        records_batch.insert_record_pair(table_name.to_string(), record_pair);
    }

    Ok(records_batch)
}

/// Converts a Metric struct to a tuple containing a timestream multi-measure Record and its
/// common attribute, comprised of the record's dimensions, measure name, and timestamp precision.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn metric_to_timestream_record_pair(
    measure_name: &str,
    metric: &Metric,
    precision: &timestream_write::types::TimeUnit,
) -> Result<RecordPair, Error> {
    let mut dimensions: Vec<timestream_write::types::Dimension> = Vec::new();
    for tag in metric.tags().iter().flatten() {
        dimensions.push(
            timestream_write::types::Dimension::builder()
                .name(tag.0.to_owned())
                .value(tag.1.to_owned())
                .build()
                .expect("Failed to build dimension"),
        )
    }

    let mut measure_values: Vec<timestream_write::types::MeasureValue> = Vec::new();
    for field in metric.fields() {
        let measure_type = get_timestream_measure_type(&field.1)?;
        measure_values.push(
            timestream_write::types::MeasureValue::builder()
                .name(field.0.to_owned())
                .value(field.1.to_string())
                .r#type(measure_type)
                .build()
                .expect("Failed to build measure"),
        );
    }

    let common_attributes = timestream_write::types::Record::builder()
        .measure_name(measure_name)
        .set_dimensions(Some(dimensions))
        .set_measure_value_type(Some(timestream_write::types::MeasureValueType::Multi))
        .set_time_unit(Some(precision.clone()))
        .build();

    let record = timestream_write::types::Record::builder()
        .set_measure_values(Some(measure_values))
        .time(metric.timestamp().to_string())
        .build();

    Ok(RecordPair {
        common_attributes,
        record,
    })
}

/// Converts a Metric struct type to a timestream MeasureValue type.
#[tracing::instrument(skip_all, level = tracing::Level::TRACE)]
pub fn get_timestream_measure_type(
    field_value: &FieldValue,
) -> Result<timestream_write::types::MeasureValueType, Error> {
    match field_value {
        FieldValue::Boolean(_) => Ok(timestream_write::types::MeasureValueType::Boolean),
        FieldValue::I64(_) => Ok(timestream_write::types::MeasureValueType::Bigint),
        FieldValue::U64(_) => Ok(timestream_write::types::MeasureValueType::Bigint),
        FieldValue::F64(_) => Ok(timestream_write::types::MeasureValueType::Double),
        FieldValue::String(_) => Ok(timestream_write::types::MeasureValueType::Varchar),
    }
}
