use std::io::{BufReader, Read};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
#[allow(deprecated)]
use datafusion::arrow::json::reader::{infer_json_schema, ValueIter};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;
use crate::arrow::json::ReaderBuilder;

use crate::error::ColumnQError;
use crate::table::TableSource;

fn json_schema_from_reader<R: Read>(r: R) -> Result<Schema, ColumnQError> {
    let mut reader = BufReader::new(r);
    Ok(infer_json_schema(&mut reader, None)?)
}

fn decode_json_from_reader<R: Read>(
    r: R,
    schema_ref: SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, ColumnQError> {
    let mut reader = BufReader::new(r);
    let mut value_reader = ValueIter::new(&mut reader, None);
    let mut batches = vec![];

    loop {
        let mut rows = vec![];
        for value in value_reader.by_ref().take(batch_size) {
            let v = value?;
            match v {
                Value::Object(_) => rows.push(v),
                _ => return Err(ColumnQError::LoadJson(format!("Row needs to be of type object, got:{v:?}"))),
            };
        }

        // value_iter take is empty
        if rows.is_empty() {
            break;
        }

        let mut decoder = ReaderBuilder::new(schema_ref.clone())
            .with_batch_size(batch_size)
            .build_decoder()
            .map_err(|e| {
                ColumnQError::LoadJson(format!("Failed create decoder: {e}"))
            })?;

        let _ = decoder.serialize(&rows[..])?;
        let batch = decoder.flush()?
            .ok_or(ColumnQError::LoadJson(format!("Failed Failed decode JSON into Arrow record batch, batch is empty")))?;
        batches.push(batch);
    }

    Ok(batches)
}

pub async fn to_mem_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    let batch_size = t.batch_size;

    let schema_ref: SchemaRef = match &t.schema {
        Some(table_schema) => Arc::new(table_schema.into()),
        // infer schema from data if not provided by user
        None => {
            let inferred_schema: Vec<Schema> =
                partitions_from_table_source!(t, json_schema_from_reader, dfctx)?;
            if inferred_schema.is_empty() {
                return Err(ColumnQError::LoadJson("failed to load schema".to_string()));
            }
            Arc::new(Schema::try_merge(inferred_schema)?)
        }
    };

    let partitions: Vec<Vec<RecordBatch>> = partitions_from_table_source!(
        t,
        |reader| decode_json_from_reader(reader, schema_ref.clone(), batch_size),
        dfctx
    )?;

    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref, partitions,
    )?)
}

#[cfg(test)]
mod tests {
    use datafusion::{datasource::TableProvider, prelude::SessionContext};

    use super::*;
    use crate::{table::TableSource, test_util::test_data_path};

    #[tokio::test]
    async fn load_simple_ndjson_file() {
        let ctx = SessionContext::new();
        let t = to_mem_table(
            &TableSource::new(
                "spacex_launches".to_string(),
                test_data_path("spacex_launches.ndjson"),
            ),
            &ctx,
        )
        .await
        .unwrap();

        let schema = t.schema();
        let fields = schema.fields();

        let mut obj_keys = fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        obj_keys.sort();
        let mut expected_obj_keys = vec![
            "fairings",
            "links",
            "static_fire_date_utc",
            "static_fire_date_unix",
            "tbd",
            "net",
            "window",
            "rocket",
            "success",
            "details",
            "crew",
            "ships",
            "capsules",
            "payloads",
            "launchpad",
            "auto_update",
            "failures",
            "flight_number",
            "name",
            "date_unix",
            "date_utc",
            "date_local",
            "date_precision",
            "upcoming",
            "cores",
            "id",
            "launch_library_id",
        ];
        expected_obj_keys.sort();

        assert_eq!(obj_keys, expected_obj_keys);
    }

    #[tokio::test]
    async fn load_simple_jsonl_file() {
        let ctx = SessionContext::new();
        let t = to_mem_table(
            &TableSource::new(
                "spacex_launches".to_string(),
                test_data_path("spacex_launches.jsonl"),
            ),
            &ctx,
        )
        .await
        .unwrap();

        let schema = t.schema();
        let fields = schema.fields();

        let mut obj_keys = fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        obj_keys.sort();
        let mut expected_obj_keys = vec![
            "fairings",
            "links",
            "static_fire_date_utc",
            "static_fire_date_unix",
            "tbd",
            "net",
            "window",
            "rocket",
            "success",
            "details",
            "crew",
            "ships",
            "capsules",
            "payloads",
            "launchpad",
            "auto_update",
            "failures",
            "flight_number",
            "name",
            "date_unix",
            "date_utc",
            "date_local",
            "date_precision",
            "upcoming",
            "cores",
            "id",
            "launch_library_id",
        ];
        expected_obj_keys.sort();

        assert_eq!(obj_keys, expected_obj_keys);
    }
}
