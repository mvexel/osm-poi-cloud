# Infra/CDK

This folder holds the CDK app for the Batch + Step Functions pipeline. It is intentionally minimal so you can grow it as you add more AWS pieces.

## Layout
- `cdk/` – CDK app (TypeScript) that defines the Step Functions state machine and any supporting IAM/Log resources.
- Existing runtime code stays where it is (`sharding/`, `batch/`, `tiles/`); only orchestration moves here.

## Prerequisites
- Node.js 18+
- AWS credentials with permission to deploy IAM, Step Functions, Batch, and CloudWatch Logs
- (One-time) CDK bootstrap in the target account/region: `cdk bootstrap aws://<ACCOUNT>/<REGION>`

## Quick start
```sh
cd infra/cdk
npm install
npm run synth   # generates CloudFormation
npm run deploy  # deploys the stack
```

## Configuring values
The stack reads context values from `cdk.json` (edit before deploy) or via `-c key=value` on the CLI. Defaults are tailored to the existing shell scripts.

| Context key | Default | Meaning |
|-------------|---------|---------|
| `projectName` | `osm-h3` | Prefix for names/log groups |
| `jobQueueName` | `osm-h3-queue` | Batch job queue name |
| `downloadJobDefinition` | `osm-h3-job` | Job definition that downloads the planet file |
| `sharderJobDefinition` | `osm-h3-job` | Job definition that runs the Rust sharder |
| `processJobDefinition` | `osm-h3-job` | Job definition that processes the whole planet after sharding |
| `postprocessJobDefinition` | `osm-h3-merge-job` | Optional merge/postprocess job |
| `stateMachineName` | `osm-h3-pipeline` | Name of the Step Functions state machine |
| `logRetentionDays` | `30` | CloudWatch Logs retention for the state machine |

If your job definitions live in a different account/region or have different names, override them with CLI context: `npm run deploy -- -c regionJobDefinition=my-job-def`.

## Expected state machine input
```json
{
  "runId": "weekly-2025-12-11",
  "shardPrefix": "s3://osm-h3-data/weekly-2025-12-11/shards/"
}
```
- `runId` is forwarded to every Batch job as `RUN_ID`.
- `shardPrefix` is forwarded as `SHARD_PREFIX` so jobs know where to read/write shards.

## What gets deployed
- CloudWatch log group for the state machine
- IAM role for Step Functions with permissions to submit/describe/terminate Batch jobs and write logs
- State machine: Download → Shard → Process (whole planet) → optional PostProcess → Success/Fail, with retries on transient errors

## Next steps
- Wire an EventBridge schedule to kick off weekly runs with your preferred `runId` and shard prefix.
- Add SNS/Slack notifications if you want alerts on failure (the stack is ready for an SNS topic ARN to be added later).
