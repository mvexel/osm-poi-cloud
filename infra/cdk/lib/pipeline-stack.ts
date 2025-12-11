import * as cdk from 'aws-cdk-lib';
import { Duration, StackProps } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';

export class PipelineStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        const projectName = this.node.tryGetContext('projectName') ?? 'osm-h3';
        const jobQueueName = this.node.tryGetContext('jobQueueName') ?? `${projectName}-queue`;

        const jobQueueArn = this.node.tryGetContext('jobQueueArn') ?? this.formatArn({
            service: 'batch',
            resource: 'job-queue',
            resourceName: jobQueueName,
        });

        const downloadJobDefinition = this.node.tryGetContext('downloadJobDefinition') ?? `${projectName}-job`;
        const sharderJobDefinition = this.node.tryGetContext('sharderJobDefinition') ?? `${projectName}-job`;
        const processJobDefinition = this.node.tryGetContext('processJobDefinition') ?? `${projectName}-job`;
        const postprocessJobDefinition = this.node.tryGetContext('postprocessJobDefinition');
        const stateMachineName = this.node.tryGetContext('stateMachineName') ?? `${projectName}-pipeline`;
        const logRetentionDays = Number(this.node.tryGetContext('logRetentionDays') ?? 30);
        const logRetention = (() => {
            switch (logRetentionDays) {
                case 1:
                    return logs.RetentionDays.ONE_DAY;
                case 3:
                    return logs.RetentionDays.THREE_DAYS;
                case 5:
                    return logs.RetentionDays.FIVE_DAYS;
                case 7:
                    return logs.RetentionDays.ONE_WEEK;
                case 14:
                    return logs.RetentionDays.TWO_WEEKS;
                case 30:
                    return logs.RetentionDays.ONE_MONTH;
                case 60:
                    return logs.RetentionDays.TWO_MONTHS;
                case 90:
                    return logs.RetentionDays.THREE_MONTHS;
                case 120:
                    return logs.RetentionDays.FOUR_MONTHS;
                case 150:
                    return logs.RetentionDays.FIVE_MONTHS;
                case 180:
                    return logs.RetentionDays.SIX_MONTHS;
                case 365:
                    return logs.RetentionDays.ONE_YEAR;
                case 400:
                    return logs.RetentionDays.THIRTEEN_MONTHS;
                case 545:
                    return logs.RetentionDays.EIGHTEEN_MONTHS;
                case 1827:
                    return logs.RetentionDays.FIVE_YEARS;
                case 3650:
                    return logs.RetentionDays.TEN_YEARS;
                default:
                    return logs.RetentionDays.ONE_MONTH;
            }
        })();

        const logGroup = new logs.LogGroup(this, 'StateMachineLogGroup', {
            logGroupName: `/aws/vendedlogs/states/${stateMachineName}`,
            retention: logRetention,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
        });

        const stateMachineRole = new iam.Role(this, 'StateMachineRole', {
            roleName: `${projectName}-stepfn-role`,
            assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
        });

        stateMachineRole.addToPolicy(
            new iam.PolicyStatement({
                actions: ['batch:SubmitJob', 'batch:DescribeJobs', 'batch:TerminateJob'],
                resources: ['*'],
            }),
        );

        logGroup.grantWrite(stateMachineRole);

        const failure = new sfn.Fail(this, 'NotifyFailure', {
            cause: 'PipelineFailed',
            error: 'A step reported failure',
        });

        const success = new sfn.Succeed(this, 'NotifySuccess');

        const download = new tasks.BatchSubmitJob(this, 'DownloadPlanet', {
            jobDefinitionArn: downloadJobDefinition,
            jobName: sfn.JsonPath.format('download-{}', sfn.JsonPath.stringAt('$.runId')),
            jobQueueArn,
            containerOverrides: {
                environment: {
                    RUN_ID: sfn.JsonPath.stringAt('$.runId'),
                },
            },
            attempts: 2,
            timeout: Duration.hours(2),
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
        });
        download.addRetry({ errors: ['States.ALL'], interval: Duration.seconds(60), maxAttempts: 3, backoffRate: 2 });
        download.addCatch(failure, { resultPath: '$.error' });

        const shard = new tasks.BatchSubmitJob(this, 'ShardPlanet', {
            jobDefinitionArn: sharderJobDefinition,
            jobName: sfn.JsonPath.format('shard-{}', sfn.JsonPath.stringAt('$.runId')),
            jobQueueArn,
            containerOverrides: {
                environment: {
                    RUN_ID: sfn.JsonPath.stringAt('$.runId'),
                    SHARD_PREFIX: sfn.JsonPath.stringAt('$.shardPrefix'),
                },
            },
            attempts: 2,
            timeout: Duration.hours(4),
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
        });
        shard.addRetry({ errors: ['States.ALL'], interval: Duration.seconds(60), maxAttempts: 3, backoffRate: 2 });
        shard.addCatch(failure, { resultPath: '$.error' });

        const processPlanet = new tasks.BatchSubmitJob(this, 'ProcessPlanet', {
            jobDefinitionArn: processJobDefinition,
            jobName: sfn.JsonPath.format('process-{}', sfn.JsonPath.stringAt('$.runId')),
            jobQueueArn,
            containerOverrides: {
                environment: {
                    RUN_ID: sfn.JsonPath.stringAt('$.runId'),
                    SHARD_PREFIX: sfn.JsonPath.stringAt('$.shardPrefix'),
                },
            },
            attempts: 2,
            timeout: Duration.hours(6),
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
        });
        processPlanet.addRetry({ errors: ['States.ALL'], interval: Duration.seconds(60), maxAttempts: 2, backoffRate: 2 });
        processPlanet.addCatch(failure, { resultPath: '$.error' });

        let chain = sfn.Chain.start(download).next(shard).next(processPlanet);

        if (postprocessJobDefinition) {
            const postProcess = new tasks.BatchSubmitJob(this, 'PostProcess', {
                jobDefinitionArn: postprocessJobDefinition,
                jobName: sfn.JsonPath.format('post-{}', sfn.JsonPath.stringAt('$.runId')),
                jobQueueArn,
                containerOverrides: {
                    environment: {
                        RUN_ID: sfn.JsonPath.stringAt('$.runId'),
                        SHARD_PREFIX: sfn.JsonPath.stringAt('$.shardPrefix'),
                    },
                },
                attempts: 1,
                timeout: Duration.hours(2),
                integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            });
            postProcess.addCatch(failure, { resultPath: '$.error' });
            chain = chain.next(postProcess);
        }

        chain = chain.next(success);

        new sfn.StateMachine(this, 'PipelineStateMachine', {
            stateMachineName,
            definitionBody: sfn.DefinitionBody.fromChainable(chain),
            role: stateMachineRole,
            tracingEnabled: true,
            logs: {
                destination: logGroup,
                level: sfn.LogLevel.ALL,
            },
        });
    }
}
