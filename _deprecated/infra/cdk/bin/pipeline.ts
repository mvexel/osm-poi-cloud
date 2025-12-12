#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { InfrastructureStack } from '../lib/infrastructure-stack';

const app = new cdk.App();

// Environment-agnostic stacks - CDK will use credentials from AWS CLI/environment.
// Only pass explicit values if they are provided via env vars (avoids partially
// specified environments where CDK cannot resolve the account).
const envAccount = process.env.CDK_DEFAULT_ACCOUNT ?? process.env.AWS_ACCOUNT_ID;
const envRegion = process.env.CDK_DEFAULT_REGION ?? process.env.AWS_REGION;
const env: cdk.Environment | undefined =
    envAccount && envRegion
        ? {
              account: envAccount,
              region: envRegion,
          }
        : undefined;

const projectName = app.node.tryGetContext('projectName') ?? 'osm-h3';
// Infrastructure stack: S3, ECR, Batch, IAM
new InfrastructureStack(app, 'OsmH3InfraStack', {
    env,
    projectName,
    description: 'OSM-H3 infrastructure: S3, ECR, Batch compute environment, job definitions',
});
