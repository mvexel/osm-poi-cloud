import * as cdk from 'aws-cdk-lib';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

export interface InfrastructureStackProps extends cdk.StackProps {
    projectName?: string;
}

/**
 * Core infrastructure for OSM-H3 pipeline:
 * - S3 bucket for data storage
 * - ECR repositories for container images
 * - VPC with public subnets
 * - Batch compute environment and job queue
 * - Job definitions for each stage
 * - IAM roles with least-privilege permissions
 */
export class InfrastructureStack extends cdk.Stack {
    public readonly bucket: s3.Bucket;
    public readonly processorRepo: ecr.Repository;
    public readonly sharderRepo: ecr.Repository;
    public readonly tilesRepo: ecr.Repository;
    public readonly jobQueue: batch.JobQueue;
    public readonly downloadJobDef: batch.EcsJobDefinition;
    public readonly sharderJobDef: batch.EcsJobDefinition;
    public readonly processorJobDef: batch.EcsJobDefinition;
    public readonly mergeJobDef: batch.EcsJobDefinition;
    public readonly tilesJobDef: batch.EcsJobDefinition;

    constructor(scope: Construct, id: string, props?: InfrastructureStackProps) {
        super(scope, id, props);

        const projectName = props?.projectName ?? 'osm-h3';

        // ============================================================
        // S3 Bucket
        // ============================================================
        this.bucket = new s3.Bucket(this, 'DataBucket', {
            bucketName: `${projectName}-data-${this.account}`,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
            autoDeleteObjects: false,
            versioned: false,
            intelligentTieringConfigurations: [
                {
                    name: 'auto-archive',
                    archiveAccessTierTime: cdk.Duration.days(90),
                    deepArchiveAccessTierTime: cdk.Duration.days(180),
                },
            ],
            lifecycleRules: [
                {
                    id: 'cleanup-incomplete-uploads',
                    abortIncompleteMultipartUploadAfter: cdk.Duration.days(7),
                },
                {
                    id: 'expire-old-runs',
                    prefix: 'runs/',
                    expiration: cdk.Duration.days(30),
                },
            ],
            cors: [
                {
                    allowedMethods: [s3.HttpMethods.GET, s3.HttpMethods.HEAD],
                    allowedOrigins: ['*'],
                    allowedHeaders: ['*'],
                },
            ],
        });

        // ============================================================
        // ECR Repositories
        // ============================================================
        this.processorRepo = new ecr.Repository(this, 'ProcessorRepo', {
            repositoryName: `${projectName}-processor`,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
            imageScanOnPush: true,
            lifecycleRules: [
                {
                    maxImageCount: 10,
                    rulePriority: 1,
                    description: 'Keep only 10 most recent images',
                },
            ],
        });

        this.sharderRepo = new ecr.Repository(this, 'SharderRepo', {
            repositoryName: `${projectName}-sharder`,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
            imageScanOnPush: true,
            lifecycleRules: [{ maxImageCount: 10, rulePriority: 1 }],
        });

        this.tilesRepo = new ecr.Repository(this, 'TilesRepo', {
            repositoryName: `${projectName}-tiles`,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
            imageScanOnPush: true,
            lifecycleRules: [{ maxImageCount: 10, rulePriority: 1 }],
        });

        // ============================================================
        // VPC - create a simple VPC for Batch
        // ============================================================
        // Using a new VPC avoids context lookups that require account/region at synth time
        const vpc = new ec2.Vpc(this, 'BatchVpc', {
            vpcName: `${projectName}-vpc`,
            maxAzs: 2,
            natGateways: 0, // Use public subnets only to avoid NAT costs
            subnetConfiguration: [
                {
                    name: 'Public',
                    subnetType: ec2.SubnetType.PUBLIC,
                    cidrMask: 24,
                },
            ],
        });

        // Security group for Batch instances
        const batchSecurityGroup = new ec2.SecurityGroup(this, 'BatchSecurityGroup', {
            vpc,
            securityGroupName: `${projectName}-batch-sg`,
            description: 'Security group for OSM-H3 Batch compute instances',
            allowAllOutbound: true,
        });

        // ============================================================
        // IAM Roles
        // ============================================================

        // Execution role for ECS tasks (pulling images, CloudWatch logs)
        const executionRole = new iam.Role(this, 'ExecutionRole', {
            roleName: `${projectName}-execution-role`,
            assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName(
                    'service-role/AmazonECSTaskExecutionRolePolicy'
                ),
            ],
        });

        // Job role for container tasks (S3 access)
        const jobRole = new iam.Role(this, 'JobRole', {
            roleName: `${projectName}-job-role`,
            assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
        });

        // Grant S3 access to job role
        this.bucket.grantReadWrite(jobRole);

        // Also allow listing (needed for glob patterns)
        jobRole.addToPolicy(
            new iam.PolicyStatement({
                actions: ['s3:ListBucket'],
                resources: [this.bucket.bucketArn],
            })
        );

        // ============================================================
        // CloudWatch Log Group
        // ============================================================
        const logGroup = new logs.LogGroup(this, 'BatchLogGroup', {
            logGroupName: `/aws/batch/${projectName}`,
            retention: logs.RetentionDays.TWO_WEEKS,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
        });

        // ============================================================
        // Batch Compute Environment
        // ============================================================
        const computeEnv = new batch.ManagedEc2EcsComputeEnvironment(this, 'ComputeEnv', {
            computeEnvironmentName: `${projectName}-compute`,
            vpc,
            vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
            securityGroups: [batchSecurityGroup],
            minvCpus: 0,
            maxvCpus: 256,
            spot: true,
            spotBidPercentage: 80,
            allocationStrategy: batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
            instanceTypes: [
                ec2.InstanceType.of(ec2.InstanceClass.M6I, ec2.InstanceSize.XLARGE),
                ec2.InstanceType.of(ec2.InstanceClass.M6I, ec2.InstanceSize.XLARGE2),
                ec2.InstanceType.of(ec2.InstanceClass.M5, ec2.InstanceSize.XLARGE),
                ec2.InstanceType.of(ec2.InstanceClass.M5, ec2.InstanceSize.XLARGE2),
                ec2.InstanceType.of(ec2.InstanceClass.R6I, ec2.InstanceSize.XLARGE),
                ec2.InstanceType.of(ec2.InstanceClass.R5, ec2.InstanceSize.XLARGE),
            ],
            useOptimalInstanceClasses: false,
        });

        // ============================================================
        // Job Queue
        // ============================================================
        this.jobQueue = new batch.JobQueue(this, 'JobQueue', {
            jobQueueName: `${projectName}-queue`,
            priority: 1,
            computeEnvironments: [
                {
                    computeEnvironment: computeEnv,
                    order: 1,
                },
            ],
        });

        // ============================================================
        // Job Definitions
        // ============================================================

        // Helper to create container definition
        const createJobDef = (
            name: string,
            repo: ecr.Repository,
            vcpus: number,
            memoryMiB: number,
            timeoutMinutes: number,
            command?: string[]
        ): batch.EcsJobDefinition => {
            const container = new batch.EcsEc2ContainerDefinition(this, `${name}Container`, {
                image: ecs.ContainerImage.fromEcrRepository(repo, 'latest'),
                cpu: vcpus,
                memory: cdk.Size.mebibytes(memoryMiB),
                jobRole,
                executionRole,
                logging: ecs.LogDrivers.awsLogs({
                    logGroup,
                    streamPrefix: name.toLowerCase(),
                }),
                command,
            });

            return new batch.EcsJobDefinition(this, `${name}JobDef`, {
                jobDefinitionName: `${projectName}-${name.toLowerCase()}`,
                container,
                timeout: cdk.Duration.minutes(timeoutMinutes),
                retryAttempts: 2,
            });
        };

        // Download job: fetches planet.osm.pbf
        this.downloadJobDef = createJobDef('Download', this.processorRepo, 2, 4096, 180);

        // Sharder job: runs Rust H3 sharder
        this.sharderJobDef = createJobDef('Sharder', this.sharderRepo, 4, 32768, 360);

        // Processor job: processes individual shards (fan-out workers)
        this.processorJobDef = createJobDef('Processor', this.processorRepo, 2, 8192, 120);

        // Merge job: combines shard outputs
        this.mergeJobDef = createJobDef('Merge', this.processorRepo, 4, 16384, 60);

        // Tiles job: generates PMTiles
        this.tilesJobDef = createJobDef('Tiles', this.tilesRepo, 4, 16384, 120);

        // ============================================================
        // Outputs
        // ============================================================
        new cdk.CfnOutput(this, 'BucketName', {
            value: this.bucket.bucketName,
            exportName: `${projectName}-bucket-name`,
        });

        new cdk.CfnOutput(this, 'ProcessorRepoUri', {
            value: this.processorRepo.repositoryUri,
            exportName: `${projectName}-processor-repo-uri`,
        });

        new cdk.CfnOutput(this, 'SharderRepoUri', {
            value: this.sharderRepo.repositoryUri,
            exportName: `${projectName}-sharder-repo-uri`,
        });

        new cdk.CfnOutput(this, 'TilesRepoUri', {
            value: this.tilesRepo.repositoryUri,
            exportName: `${projectName}-tiles-repo-uri`,
        });

        new cdk.CfnOutput(this, 'JobQueueArn', {
            value: this.jobQueue.jobQueueArn,
            exportName: `${projectName}-job-queue-arn`,
        });
    }
}
