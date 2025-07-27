use aws_lambda_events::event::s3::S3Event;
use aws_sdk_s3::Client as S3Client;
use lambda_runtime::{tracing, Error, LambdaEvent};

fn get_bucket_name(event: &S3Event) -> Result<String, Error> {
    event
        .records
        .first()
        .and_then(|record| record.s3.bucket.name.as_ref())
        .cloned()
        .ok_or_else(|| lambda_runtime::Error::from("No bucket name found in S3 event"))
}

async fn process_s3_records(s3_client: &S3Client, event: &S3Event) -> Result<f64, Error> {
    let mut total_bytes: i64 = 0;
    let mut delete_obj_ids: Vec<aws_sdk_s3::types::ObjectIdentifier> = Vec::new();
    let bucket_name = get_bucket_name(event)?;
    for record in &event.records {
        total_bytes += &record.s3.object.size.unwrap_or_default();
        let key = record.s3.object.key.clone().unwrap_or_default().replace('+', " ");
        tracing::info!("Deleting {}/{}", bucket_name, key);
        let obj_id = aws_sdk_s3::types::ObjectIdentifier::builder()
            // need to encode the key to handle special characters
            .key(key)
            .build()
            .map_err(|e| lambda_runtime::Error::from(e.to_string()))?;
        delete_obj_ids.push(obj_id);
    }
    s3_client
        .delete_objects()
        .bucket(bucket_name)
        .delete(
            aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(delete_obj_ids))
                .build()
                .map_err(|e| lambda_runtime::Error::from(e.to_string()))?,
        )
        .send()
        .await?;
    Ok(total_bytes as f64 / 1024.0 / 1024.0)

}

pub(crate) async fn function_handler(
    event: LambdaEvent<S3Event>,
    s3_client: &S3Client
) -> Result<(), Error> {
    let payload = event.payload;
    if payload.records.is_empty() {
        tracing::warn!("No records found in S3 event");
        return Ok(());
    }
    let total_mb = process_s3_records(s3_client, &payload).await?;
    tracing::info!("Total size of deleted objects: {:.2} MB", total_mb);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lambda_events::s3::S3EventRecord;
    use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
    use aws_smithy_mocks::{mock, mock_client};
    use lambda_runtime::{Context, LambdaEvent};

    #[tokio::test]
    async fn test_event_handler() {
        let record = S3EventRecord {
            s3: aws_lambda_events::event::s3::S3Entity {
                bucket: aws_lambda_events::event::s3::S3Bucket {
                    name: Some("test-bucket".to_string()),
                    ..Default::default()
                },
                object: aws_lambda_events::event::s3::S3Object {
                    key: Some("test object".to_string()),
                    size: Some(1234),
                    ..Default::default()
                },
                schema_version: Some("1.0".to_string()),
                configuration_id: Some("config-id".to_string()),
            },
            ..Default::default()
        };
        let event = LambdaEvent {
            payload: S3Event {
                records: vec![record],
            },
            context: Context::default(),
        };
        let delete_objects_rule = mock!(aws_sdk_s3::Client::delete_objects).then_output(|| {
            DeleteObjectsOutput::builder()
                .set_deleted(Some(vec![]))
                .build()
        });
        let s3 = mock_client!(aws_sdk_s3, [&delete_objects_rule]);
        let response = function_handler(event, &s3).await.unwrap();
        assert_eq!(delete_objects_rule.num_calls(), 1);
        assert_eq!((), response);
    }
}
