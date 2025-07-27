use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use lambda_runtime::{run, service_fn, tracing, Error};
mod event_handler;
use event_handler::function_handler;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::subscriber::fmt().json().init();
    let shared_config = aws_config::load_defaults(BehaviorVersion::v2025_01_17()).await;
    let s3_client = S3Client::new(&shared_config);
    run(service_fn(|event| function_handler(event, &s3_client))).await
}
