<?php

declare(strict_types=1);

use Aws\Exception\AwsException;
use Aws\MockHandler;
use Aws\Result;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use Aws\Sts\StsClient;
use GuzzleHttp\Psr7\Response;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\SnsSqsTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Spoolrail\Spoolrail\SnsSqs\ConnectionConfig;
use Spoolrail\Spoolrail\SnsSqs\QueuePolicy;
use Spoolrail\Spoolrail\SnsSqs\Topology;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

test('plans before creating high-throughput FIFO resources and a raw fanout route', function (): void {
    // --- Arrange ---
    $snsCommands = [];
    $sqsCommands = [];
    $snsHandler = new MockHandler;
    $sns = new SnsClient(snsSqsTopologyClientOptions($snsHandler));
    $snsHandler->append(
        new AwsException(
            'Topic not found.',
            $sns->getCommand('GetTopicAttributes'),
            ['response' => new Response(404), 'code' => 'NotFound'],
        ),
        function ($command) use (&$snsCommands): Result {
            $snsCommands[] = [$command->getName(), $command->toArray()];

            return new Result([
                'TopicArn' => 'arn:aws:sns:us-east-1:123456789012:orders.fifo',
            ]);
        },
        function ($command) use (&$snsCommands): Result {
            $snsCommands[] = [$command->getName(), $command->toArray()];

            return new Result(['SubscriptionArn' => 'arn:aws:sns:subscription']);
        },
    );
    $sqsHandler = new MockHandler([
        new Result(['QueueUrls' => []]),
        function ($command) use (&$sqsCommands): Result {
            $sqsCommands[] = [$command->getName(), $command->toArray()];

            return new Result([
                'QueueUrl' => 'http://localhost:4566/123456789012/application-a-warehouse.fifo',
            ]);
        },
    ]);
    $stsHandler = new MockHandler([
        new Result(['Account' => '123456789012']),
    ]);
    $topology = snsSqsTopology($sns, $sqsHandler, $stsHandler);

    // --- Act ---
    $plan = $topology->planSync([snsSqsSubscription()], 'application-a');
    $commandsBeforeApply = [
        $snsHandler->getLastCommand()->getName(),
        $sqsHandler->getLastCommand()->getName(),
        $stsHandler->getLastCommand()->getName(),
    ];
    $plan->apply();

    // --- Assert ---
    expect($commandsBeforeApply)->toBe([
        'GetTopicAttributes',
        'ListQueues',
        'GetCallerIdentity',
    ]);
    expect($snsCommands[0][0])->toBe('CreateTopic');
    expect($snsCommands[0][1]['Name'])->toBe('orders.fifo');
    expect($snsCommands[0][1]['Attributes'])->toBe([
        'FifoTopic' => 'true',
        'FifoThroughputScope' => 'MessageGroup',
    ]);
    expect($sqsCommands[0][0])->toBe('CreateQueue');
    expect($sqsCommands[0][1]['QueueName'])->toBe('application-a-warehouse.fifo');
    expect($sqsCommands[0][1]['Attributes'])->toMatchArray([
        'FifoQueue' => 'true',
        'DeduplicationScope' => 'messageGroup',
        'FifoThroughputLimit' => 'perMessageGroupId',
        'VisibilityTimeout' => '30',
    ]);
    expect($snsCommands[1][0])->toBe('Subscribe');
    expect($snsCommands[1][1])->toMatchArray([
        'TopicArn' => 'arn:aws:sns:us-east-1:123456789012:orders.fifo',
        'Protocol' => 'sqs',
        'Endpoint' => 'arn:aws:sqs:us-east-1:123456789012:application-a-warehouse.fifo',
        'Attributes' => ['RawMessageDelivery' => 'true'],
        'ReturnSubscriptionArn' => true,
    ]);

    $policy = json_decode(
        $sqsCommands[0][1]['Attributes']['Policy'],
        true,
        flags: JSON_THROW_ON_ERROR,
    );
    expect($policy['Statement'][0])->toMatchArray([
        'Sid' => 'SpoolrailSnsPublish',
        'Action' => 'sqs:SendMessage',
        'Resource' => 'arn:aws:sqs:us-east-1:123456789012:application-a-warehouse.fifo',
        'Condition' => [
            'ArnEquals' => [
                'aws:SourceArn' => 'arn:aws:sns:us-east-1:123456789012:orders.fifo',
            ],
        ],
    ]);
});

test('plans one shared topic with independent queues for multiple subscriptions', function (): void {
    // --- Arrange ---
    $snsCommands = [];
    $sqsCommands = [];
    $snsHandler = new MockHandler;
    $sns = new SnsClient(snsSqsTopologyClientOptions($snsHandler));
    $snsHandler->append(
        new AwsException(
            'Topic not found.',
            $sns->getCommand('GetTopicAttributes'),
            ['response' => new Response(404), 'code' => 'NotFound'],
        ),
        function ($command) use (&$snsCommands): Result {
            $snsCommands[] = [$command->getName(), $command->toArray()];

            return new Result([
                'TopicArn' => 'arn:aws:sns:us-east-1:123456789012:orders.fifo',
            ]);
        },
        function ($command) use (&$snsCommands): Result {
            $snsCommands[] = [$command->getName(), $command->toArray()];

            return new Result(['SubscriptionArn' => 'arn:aws:sns:first-subscription']);
        },
        function ($command) use (&$snsCommands): Result {
            $snsCommands[] = [$command->getName(), $command->toArray()];

            return new Result(['SubscriptionArn' => 'arn:aws:sns:second-subscription']);
        },
    );
    $sqsHandler = new MockHandler([
        new Result(['QueueUrls' => []]),
        function ($command) use (&$sqsCommands): Result {
            $sqsCommands[] = [$command->getName(), $command->toArray()];

            return new Result([
                'QueueUrl' => 'http://localhost:4566/123456789012/application-a-warehouse.fifo',
            ]);
        },
        function ($command) use (&$sqsCommands): Result {
            $sqsCommands[] = [$command->getName(), $command->toArray()];

            return new Result([
                'QueueUrl' => 'http://localhost:4566/123456789012/application-a-billing.fifo',
            ]);
        },
    ]);
    $stsHandler = new MockHandler([
        new Result(['Account' => '123456789012']),
    ]);
    $topology = snsSqsTopology($sns, $sqsHandler, $stsHandler);

    // --- Act ---
    $plan = $topology->planSync([
        snsSqsSubscription('warehouse'),
        snsSqsSubscription('billing'),
    ], 'application-a');
    $plan->apply();

    // --- Assert ---
    expect(array_column($snsCommands, 0))->toBe(['CreateTopic', 'Subscribe', 'Subscribe']);
    expect($snsCommands[0][1]['Name'])->toBe('orders.fifo');
    expect([
        $snsCommands[1][1]['Endpoint'],
        $snsCommands[2][1]['Endpoint'],
    ])->toBe([
        'arn:aws:sqs:us-east-1:123456789012:application-a-warehouse.fifo',
        'arn:aws:sqs:us-east-1:123456789012:application-a-billing.fifo',
    ]);
    expect(array_column($sqsCommands, 0))->toBe(['CreateQueue', 'CreateQueue']);
    expect([
        $sqsCommands[0][1]['QueueName'],
        $sqsCommands[1][1]['QueueName'],
    ])->toBe([
        'application-a-warehouse.fifo',
        'application-a-billing.fifo',
    ]);
});

test('adds a raw route to existing resources without replacing unrelated queue policy', function (): void {
    // --- Arrange ---
    $topicArn = 'arn:aws:sns:us-east-1:123456789012:orders.fifo';
    $queueArn = 'arn:aws:sqs:us-east-1:123456789012:application-a-warehouse.fifo';
    $queueUrl = 'http://localhost:4566/123456789012/application-a-warehouse.fifo';
    $existingStatement = [
        'Sid' => 'AllowMonitoring',
        'Effect' => 'Allow',
        'Principal' => ['AWS' => 'arn:aws:iam::123456789012:role/monitoring'],
        'Action' => 'sqs:GetQueueAttributes',
        'Resource' => $queueArn,
    ];
    $existingPolicy = json_encode([
        'Version' => '2012-10-17',
        'Statement' => [$existingStatement],
    ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    $snsHandler = new MockHandler([
        new Result(['Attributes' => [
            'FifoTopic' => 'true',
            'FifoThroughputScope' => 'MessageGroup',
        ]]),
        new Result(['Subscriptions' => []]),
        new Result(['SubscriptionArn' => 'arn:aws:sns:subscription']),
    ]);
    $sqsHandler = new MockHandler([
        new Result(['QueueUrls' => [$queueUrl]]),
        new Result(['Attributes' => [
            'QueueArn' => $queueArn,
            'FifoQueue' => 'true',
            'DeduplicationScope' => 'messageGroup',
            'FifoThroughputLimit' => 'perMessageGroupId',
            'Policy' => $existingPolicy,
        ]]),
        new Result,
    ]);
    $topology = snsSqsTopology(
        new SnsClient(snsSqsTopologyClientOptions($snsHandler)),
        $sqsHandler,
        new MockHandler,
    );

    // --- Act ---
    $topology->planSync([snsSqsSubscription()], 'application-a')->apply();

    // --- Assert ---
    $setQueueAttributes = $sqsHandler->getLastCommand();
    $updatedPolicy = json_decode(
        $setQueueAttributes->get('Attributes')['Policy'],
        true,
        flags: JSON_THROW_ON_ERROR,
    );

    expect($setQueueAttributes->getName())->toBe('SetQueueAttributes');
    expect($setQueueAttributes->get('QueueUrl'))->toBe($queueUrl);
    expect($setQueueAttributes->get('Attributes')['VisibilityTimeout'])->toBe('30');
    expect($updatedPolicy['Statement'][0])->toBe($existingStatement);
    expect($updatedPolicy['Statement'][1])->toMatchArray([
        'Sid' => 'SpoolrailSnsPublish',
        'Action' => 'sqs:SendMessage',
        'Resource' => $queueArn,
        'Condition' => ['ArnEquals' => ['aws:SourceArn' => $topicArn]],
    ]);
    expect($snsHandler->getLastCommand()->toArray())->toMatchArray([
        'TopicArn' => $topicArn,
        'Protocol' => 'sqs',
        'Endpoint' => $queueArn,
        'Attributes' => ['RawMessageDelivery' => 'true'],
    ]);
});

test('rejects an existing SNS route without raw message delivery', function (): void {
    // --- Arrange ---
    $topicArn = 'arn:aws:sns:us-east-1:123456789012:orders.fifo';
    $queueArn = 'arn:aws:sqs:us-east-1:123456789012:application-a-warehouse.fifo';
    $queueUrl = 'http://localhost:4566/123456789012/application-a-warehouse.fifo';
    $queuePolicy = json_encode([
        'Version' => '2012-10-17',
        'Statement' => [[
            'Sid' => 'SpoolrailSnsPublish',
            'Effect' => 'Allow',
            'Principal' => ['Service' => 'sns.amazonaws.com'],
            'Action' => 'sqs:SendMessage',
            'Resource' => $queueArn,
            'Condition' => ['ArnEquals' => ['aws:SourceArn' => $topicArn]],
        ]],
    ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    $snsHandler = new MockHandler([
        new Result(['Attributes' => [
            'FifoTopic' => 'true',
            'FifoThroughputScope' => 'MessageGroup',
        ]]),
        new Result(['Subscriptions' => [[
            'SubscriptionArn' => 'arn:aws:sns:subscription',
            'Protocol' => 'sqs',
            'Endpoint' => $queueArn,
        ]]]),
        new Result(['Attributes' => ['RawMessageDelivery' => 'false']]),
    ]);
    $sqsHandler = new MockHandler([
        new Result(['QueueUrls' => [$queueUrl]]),
        new Result(['Attributes' => [
            'QueueArn' => $queueArn,
            'FifoQueue' => 'true',
            'DeduplicationScope' => 'messageGroup',
            'FifoThroughputLimit' => 'perMessageGroupId',
            'Policy' => $queuePolicy,
        ]]),
    ]);
    $topology = snsSqsTopology(
        new SnsClient(snsSqsTopologyClientOptions($snsHandler)),
        $sqsHandler,
        new MockHandler,
    );

    // --- Act ---
    $action = fn (): TopologyPlan => $topology->planSync([snsSqsSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(
        SnsSqsTopologyException::class,
        'must enable raw message delivery',
    );
    expect($snsHandler->getLastCommand()->getName())->toBe('GetSubscriptionAttributes');
    expect($sqsHandler->getLastCommand()->getName())->toBe('GetQueueAttributes');
});

test('rejects normal-throughput FIFO topics without mutating them', function (): void {
    // --- Arrange ---
    $snsHandler = new MockHandler([
        new Result(['Attributes' => [
            'FifoTopic' => 'true',
            'FifoThroughputScope' => 'Topic',
        ]]),
    ]);
    $sqsHandler = new MockHandler([
        new Result(['QueueUrls' => []]),
    ]);
    $topology = snsSqsTopology(
        new SnsClient(snsSqsTopologyClientOptions($snsHandler)),
        $sqsHandler,
        new MockHandler,
    );

    // --- Act ---
    $action = fn (): TopologyPlan => $topology->planSync([snsSqsSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(SnsSqsTopologyException::class, 'FifoThroughputScope');
    expect($snsHandler->getLastCommand()->getName())->toBe('GetTopicAttributes');
    expect($sqsHandler->getLastCommand()->getName())->toBe('ListQueues');
});

test('rejects normal-throughput FIFO queues without mutating them', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/application-a-warehouse.fifo';
    $snsHandler = new MockHandler([
        new Result(['Attributes' => [
            'FifoTopic' => 'true',
            'FifoThroughputScope' => 'MessageGroup',
        ]]),
        new Result(['Subscriptions' => []]),
    ]);
    $sqsHandler = new MockHandler([
        new Result(['QueueUrls' => [$queueUrl]]),
        new Result(['Attributes' => [
            'QueueArn' => 'arn:aws:sqs:us-east-1:123456789012:application-a-warehouse.fifo',
            'FifoQueue' => 'true',
            'DeduplicationScope' => 'queue',
            'FifoThroughputLimit' => 'perQueue',
        ]]),
    ]);
    $topology = snsSqsTopology(
        new SnsClient(snsSqsTopologyClientOptions($snsHandler)),
        $sqsHandler,
        new MockHandler,
    );

    // --- Act ---
    $action = fn (): TopologyPlan => $topology->planSync([snsSqsSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(SnsSqsTopologyException::class, 'DeduplicationScope');
    expect($snsHandler->getLastCommand()->getName())->toBe('ListSubscriptionsByTopic');
    expect($sqsHandler->getLastCommand()->getName())->toBe('GetQueueAttributes');
});

test('refuses to create resources with credentials from the wrong owning account', function (): void {
    // --- Arrange ---
    $snsHandler = new MockHandler;
    $sns = new SnsClient(snsSqsTopologyClientOptions($snsHandler));
    $snsHandler->append(new AwsException(
        'Topic not found.',
        $sns->getCommand('GetTopicAttributes'),
        ['response' => new Response(404), 'code' => 'NotFound'],
    ));
    $sqsHandler = new MockHandler([
        new Result(['QueueUrls' => []]),
    ]);
    $stsHandler = new MockHandler([
        new Result(['Account' => '999999999999']),
    ]);
    $topology = snsSqsTopology($sns, $sqsHandler, $stsHandler);

    // --- Act ---
    $action = fn (): TopologyPlan => $topology->planSync([snsSqsSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(
        SnsSqsTopologyException::class,
        'credentials for resource-owning account [123456789012]',
    );
    expect($snsHandler->getLastCommand()->getName())->toBe('GetTopicAttributes');
    expect($sqsHandler->getLastCommand()->getName())->toBe('ListQueues');
    expect($stsHandler->getLastCommand()->getName())->toBe('GetCallerIdentity');
});

test('requests a topology retry when AWS discovery is rate limited', function (): void {
    // --- Arrange ---
    $commandClient = new SqsClient(snsSqsTopologyClientOptions(new MockHandler));
    $failure = new AwsException(
        'Rate exceeded.',
        $commandClient->getCommand('ListQueues'),
        [
            'response' => new Response(400),
            'code' => 'ThrottlingException',
        ],
    );
    $sqsHandler = new MockHandler([$failure]);
    $topology = snsSqsTopology(
        new SnsClient(snsSqsTopologyClientOptions(new MockHandler)),
        $sqsHandler,
        new MockHandler,
    );

    // --- Act ---
    $caught = null;

    try {
        $topology->planSync([snsSqsSubscription()], 'application-a');
    } catch (TopologySyncRequiresRetryException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->getPrevious())->toBeInstanceOf(SnsSqsTopologyException::class);
    expect($caught?->getPrevious()?->getPrevious())->toBe($failure);
    expect(count($sqsHandler))->toBe(0);
});

test('requests a topology retry when AWS discovery loses its response', function (): void {
    // --- Arrange ---
    $commandClient = new SqsClient(snsSqsTopologyClientOptions(new MockHandler));
    $failure = new AwsException(
        'Connection reset.',
        $commandClient->getCommand('ListQueues'),
    );
    $sqsHandler = new MockHandler([$failure]);
    $topology = snsSqsTopology(
        new SnsClient(snsSqsTopologyClientOptions(new MockHandler)),
        $sqsHandler,
        new MockHandler,
    );

    // --- Act ---
    $caught = null;

    try {
        $topology->planSync([snsSqsSubscription()], 'application-a');
    } catch (TopologySyncRequiresRetryException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->getPrevious())->toBeInstanceOf(SnsSqsTopologyException::class);
    expect($caught?->getPrevious()?->getPrevious())->toBe($failure);
    expect(count($sqsHandler))->toBe(0);
});

/**
 * @return array<string, mixed>
 */
function snsSqsTopologyClientOptions(MockHandler $handler): array
{
    return [
        'version' => 'latest',
        'region' => 'us-east-1',
        'endpoint' => 'http://localhost:4566',
        'credentials' => false,
        'handler' => $handler,
        'retries' => 0,
    ];
}

function snsSqsTopology(
    SnsClient $sns,
    MockHandler $sqsHandler,
    MockHandler $stsHandler,
): Topology {
    $config = new ConnectionConfig('snssqs', [
        'region' => 'us-east-1',
        'account_id' => '123456789012',
        'fifo' => true,
    ]);

    return new Topology(
        $config,
        $sns,
        new SqsClient(snsSqsTopologyClientOptions($sqsHandler)),
        new StsClient(snsSqsTopologyClientOptions($stsHandler)),
        new QueuePolicy,
    );
}

function snsSqsSubscription(string $name = 'warehouse'): Subscription
{
    return new Subscription(
        'orders',
        $name,
        RecordingMessageHandler::class,
        static function (): void {},
    );
}
