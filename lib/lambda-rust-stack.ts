import * as rust from '@cdklabs/aws-lambda-rust';
import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3events from 'aws-cdk-lib/aws-s3-notifications';
import { Construct } from 'constructs';

export class LambdaRustStack extends cdk.Stack {
  private readonly rustHandler: rust.RustFunction
  private readonly bucket: s3.Bucket;
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.bucket = new s3.Bucket(this, 'HelloRustBucket', {
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true
    });

    this.rustHandler = new rust.RustFunction(this, 'HelloRust', {
      binaryName: 'rust_lambda',
      entry: 'rust_lambda'
    })
    this.bucket.grantReadWrite(this.rustHandler);
    this.bucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3events.LambdaDestination(this.rustHandler));
  }
}
