mod query_common {
    use aws_sdk_timestreamquery as timestream_query;
    use aws_sdk_timestreamquery::Error;
    use aws_sdk_timestreamquery::types as types;
    use std::fs;
    use std::io::{self, Write};

    pub fn write(mut file: &fs::File, s: String) -> io::Result<()> {
        let s_formatted = format!("{}\n", s);
        file.write(s_formatted.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    pub fn process_scalar_type(data: &types::Datum) -> Result<String, String> {
        data.scalar_value.clone().ok_or_else(|| "Scalar value is None".to_string())
    }

    pub fn process_time_series_type(data: &[types::TimeSeriesDataPoint], column_info: &types::ColumnInfo) -> Result<String, String> {
        let mut value = String::new();
        for (i, datum) in data.iter().enumerate() {
            value.push_str(&datum.time);
            value.push(':');
            
            let column_type = column_info.r#type();
            let column_type_ref = column_type.as_ref().ok_or("Column type is None")?;
            let scalar_type = column_type_ref.scalar_type.to_owned();
            let scalar_type_ref = scalar_type.ok_or("Scalar type is None")?;

            let datum_value = datum.value.as_ref().ok_or("Datum value is None")?;
            let _scalar_value = datum_value.scalar_value.to_owned().ok_or("Scalar value is None")?;

            if scalar_type_ref.as_str() == "" {
                value.push_str(&process_scalar_type(datum_value)?);
            } else if let Some(array_column_info) = &column_type_ref.array_column_info {
                let array_value = datum_value.array_value.as_ref().ok_or("Array value is None")?;
                value.push_str(&process_array_type(array_value, array_column_info)?);
            } else if let Some(row_column_info) = &column_type_ref.row_column_info {
                let row_value = datum_value.row_value.as_ref().ok_or("Row value is None")?;
                value.push_str(&process_row_type(&row_value.data, row_column_info)?);
            } else {
                panic!("Bad data type");
            }

            if i != data.len() - 1 {
                value.push_str(", ");
            }
        }
        return Ok(value);
    }

    pub fn process_array_type(datum_list: &[types::Datum], column_info: &types::ColumnInfo) -> Result<String, String> {
        let mut value = String::new();
        for (i, datum) in datum_list.iter().enumerate() {

            let column_type = column_info.r#type();
            let column_type_ref = column_type.as_ref().ok_or("Column type is None")?;

            let scalar_type = column_type_ref.scalar_type.to_owned();
            let scalar_type_ref = scalar_type.ok_or("Scalar type is None")?;

            if scalar_type_ref.as_str() != "" {
                value.push_str(&process_scalar_type(&datum)?);
            } else if let Some(time_series_measure_value_column_info) =  &column_type_ref.time_series_measure_value_column_info {
                let time_series_value = datum.time_series_value.as_ref().ok_or("Time series value is None")?;
                value.push_str(&process_time_series_type(&time_series_value, time_series_measure_value_column_info)?);
            } else if let Some(array_column_info) = &column_type_ref.array_column_info {
                let array_value = datum.array_value.as_ref().ok_or("Array value is None")?;
                value.push_str("[");
                value.push_str(&process_array_type(&array_value, array_column_info)?);
                value.push_str("]");
            } else if let Some(row_column_info) = &column_type_ref.row_column_info {
                let row_value = datum.row_value.as_ref().ok_or("Row value is None")?;
                value.push_str("[");
                value.push_str(&process_row_type(&row_value.data, row_column_info)?);
                value.push_str("]");
            } else {
                panic!("Bad column type");
            }

            if i != datum_list.len() - 1 {
                value.push_str(", ");
            }
        }
        return Ok(value);
    }

    pub fn process_row_type(data: &[types::Datum], metadata: &[types::ColumnInfo]) -> Result<String, String> {
        let mut value = String::new();
        for (i, datum) in data.iter().enumerate() {
            let column_info = metadata[i].clone();
            let column_type = column_info.r#type();
            let column_type_ref = column_type.as_ref().ok_or("Column type is None")?;
            let scalar_type = column_type_ref.scalar_type.to_owned();
            let scalar_type_ref = scalar_type.ok_or("Scalar type is None")?;

            if scalar_type_ref.as_str() != "" {
                // process simple data types
                value.push_str(&process_scalar_type(&datum)?);
            } else if let Some(time_series_measure_value_column_info) = &column_type_ref.time_series_measure_value_column_info {
                let datapoint_list = datum.time_series_value.as_ref().ok_or("Time series value is None")?;
                value.push_str("[");
                value.push_str(&process_time_series_type(&datapoint_list, &time_series_measure_value_column_info)?);
                value.push_str("]");
            } else if let Some(array_column_info) = &column_type_ref.array_column_info {
                let array_value = datum.array_value.as_ref().ok_or("Array value is None")?;
                value.push_str("[");
                value.push_str(&process_array_type(&array_value, array_column_info)?);
                value.push_str("]");
            } else if let Some(row_column_info) = &column_type_ref.row_column_info {
                let row_value = datum.row_value.as_ref().ok_or("Row value is None")?;
                value.push_str("[");
                value.push_str(&process_row_type(&row_value.data, &row_column_info)?);
                value.push_str("]");
            } else {
                panic!("Bad column type");
            }

            // comma seperated column values
            if i != data.len() - 1 {
                value.push_str(", ");
            }
        }
        return Ok(value);
    }

    pub async fn run_query(query: String, client: timestream_query::Client, f: &std::fs::File, max_rows: i32) -> Result<(), String> {
        let query_builder = client.query()
                .query_string(query)
                .max_rows(max_rows);
        loop {
            let query_result = query_builder.send().await;

            match query_result {
                Ok(query_result) => {
                    parse_query_result(query_result);
                    let message = format!("Number of rows: {}", query_result.rows.len()).to_string();
                    println!("{}", message);
                    write(f, message);
                    if let Some(new_next_token) = &query_result.next_token {
                        query_builder.next_token(new_next_token.to_owned());
                    } else {
                        break;
                    }
                }

                Err(err) => {
                    let error_string = err.to_string();
                    println!("Error while querying the query {} : {}", query, error_string);
                    return Err(error_string);
                }
            }
        }
        return Ok(());

        /* for {
            queryResponse, err := querySvc.Query(context.TODO(), queryInput)
            if err != nil {
                fmt.Printf("Error while querying the query %s : %s\n", *queryPtr, err.Error())
                return err
            }

            ParseQueryResult(queryResponse, f)
            msg := fmt.Sprintf("Number of Rows: %d\n", len(queryResponse.Rows))
            fmt.Print(msg)
            Write(f, msg)

            if queryResponse.NextToken == nil {
                break
            }
            queryInput.NextToken = queryResponse.NextToken
            queryResponse, _ = querySvc.Query(context.TODO(), queryInput)
        }
            return nil */
    }

}
