#!/usr/bin/env node
"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const cdk = __importStar(require("aws-cdk-lib"));
const infrastructure_stack_1 = require("../lib/infrastructure-stack");
const app = new cdk.App();
// Environment-agnostic stacks - CDK will use credentials from AWS CLI/environment.
// Only pass explicit values if they are provided via env vars (avoids partially
// specified environments where CDK cannot resolve the account).
const envAccount = process.env.CDK_DEFAULT_ACCOUNT ?? process.env.AWS_ACCOUNT_ID;
const envRegion = process.env.CDK_DEFAULT_REGION ?? process.env.AWS_REGION;
const env = envAccount && envRegion
    ? {
        account: envAccount,
        region: envRegion,
    }
    : undefined;
const projectName = app.node.tryGetContext('projectName') ?? 'osm-h3';
// Infrastructure stack: S3, ECR, Batch, IAM
new infrastructure_stack_1.InfrastructureStack(app, 'OsmH3InfraStack', {
    env,
    projectName,
    description: 'OSM-H3 infrastructure: S3, ECR, Batch compute environment, job definitions',
});
