<?php

declare(strict_types=1);

use Aws\Exception\AwsException;
use Aws\Exception\CredentialsException;
use Aws\MockHandler;
use Aws\Result;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use GuzzleHttp\Psr7\Response;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Drivers\SnsSqsDriver;
use Spoolrail\Spoolrail\Enums\ConsumptionFailure;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\SnsSqs\ConnectionConfig;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Spoolrail\Spoolrail\TransportContext;

test('publishes FIFO messages in the default topic lane with logical deduplication identity', function (): void {
    // --- Arrange ---
    $snsHandler = new MockHandler([new Result(['MessageId' => 'transport-id'])]);
    $driver = snsSqsDriver($snsHandler);
    $body = snsSqsMessageBody();

    // --- Act ---
    $driver->publish('orders', $body, ['correlation-id' => 'A-42']);

    // --- Assert ---
    $request = array_filter(
        $snsHandler->getLastCommand()->toArray(),
        static fn (string $key): bool => ! str_starts_with($key, '@'),
        ARRAY_FILTER_USE_KEY,
    );

    expect($request)->toBe([
        'TopicArn' => 'arn:aws:sns:us-east-1:123456789012:orders.fifo',
        'Message' => $body,
        'MessageAttributes' => [
            'correlation-id' => [
                'DataType' => 'String',
                'StringValue' => 'A-42',
            ],
        ],
        'MessageGroupId' => 'spoolrail',
        'MessageDeduplicationId' => '01890a5d-ac96-774b-bcd0-48f622f3e798',
    ]);
});

test('uses a custom ordering key as the FIFO message group', function (): void {
    // --- Arrange ---
    $handler = new MockHandler([new Result]);
    $driver = snsSqsDriver($handler);

    // --- Act ---
    $driver->publish('orders', snsSqsMessageBody(), [], 'order:42');

    // --- Assert ---
    expect($handler->getLastCommand()->toArray())->toMatchArray([
        'MessageGroupId' => 'order:42',
        'MessageDeduplicationId' => '01890a5d-ac96-774b-bcd0-48f622f3e798',
    ]);
});

test('forwards a standard fair-queue group only when one is supplied', function (): void {
    // --- Arrange ---
    $handler = new MockHandler([new Result, new Result]);
    $driver = snsSqsDriver($handler, fifo: false);

    // --- Act ---
    $driver->publish('orders', snsSqsMessageBody(), [], 'tenant:42');
    $withKey = $handler->getLastCommand()->toArray();
    $driver->publish('orders', snsSqsMessageBody(), []);
    $withoutKey = $handler->getLastCommand()->toArray();

    // --- Assert ---
    expect($withKey['MessageGroupId'])->toBe('tenant:42');
    expect($withKey)->not->toHaveKey('MessageDeduplicationId');
    expect($withoutKey)->not->toHaveKeys(['MessageGroupId', 'MessageDeduplicationId']);
});

test('reports a credential resolution failure as not sent', function (): void {
    // --- Arrange ---
    $failure = new CredentialsException('No AWS credentials are available.');
    $handler = new MockHandler([$failure]);
    $driver = snsSqsDriver($handler);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', snsSqsMessageBody(), []);
    } catch (PublicationException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->outcome)->toBe(PublicationOutcome::NotSent);
    expect($caught?->getPrevious())->toBe($failure);
    expect(count($handler))->toBe(0);
});

test('reports an explicit SNS refusal as rejected after one attempt', function (): void {
    // --- Arrange ---
    $client = new SnsClient(snsSqsClientOptions());
    $failure = new AwsException(
        'Forbidden',
        $client->getCommand('Publish'),
        ['response' => new Response(403)],
    );
    $handler = new MockHandler([$failure]);
    $driver = snsSqsDriver($handler);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', snsSqsMessageBody(), []);
    } catch (PublicationException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->outcome)->toBe(PublicationOutcome::Rejected);
    expect($caught?->getPrevious())->toBe($failure);
    expect(count($handler))->toBe(0);
});

test('reports an uncertain transport failure without a hidden retry', function (): void {
    // --- Arrange ---
    $client = new SnsClient(snsSqsClientOptions());
    $failure = new AwsException(
        'Service unavailable after dispatch.',
        $client->getCommand('Publish'),
        ['response' => new Response(503)],
    );
    $handler = new MockHandler([$failure]);
    $driver = snsSqsDriver($handler);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', snsSqsMessageBody(), []);
    } catch (PublicationException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->outcome)->toBe(PublicationOutcome::Unknown);
    expect($caught?->getPrevious())->toBe($failure);
    expect(count($handler))->toBe(0);
});

test('resolves one queue URL and reuses it for receiving and settlement', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $commands = [];
    $sqsHandler = new MockHandler([
        function ($command) use (&$commands, $queueUrl): Result {
            $commands[] = [
                $command->getName(),
                $command->get('QueueUrl'),
                $command->get('QueueOwnerAWSAccountId'),
            ];

            return new Result(['QueueUrl' => $queueUrl]);
        },
        function ($command) use (&$commands): Result {
            $commands[] = [$command->getName(), $command->get('QueueUrl'), null];

            return new Result(['Messages' => [sqsDelivery()]]);
        },
        function ($command) use (&$commands): Result {
            $commands[] = [$command->getName(), $command->get('QueueUrl'), null];

            return new Result;
        },
        function ($command) use (&$commands): never {
            $commands[] = [$command->getName(), $command->get('QueueUrl'), null];

            throw new RuntimeException('Stop after the settled delivery.');
        },
    ]);
    $contexts = [];
    $driver = snsSqsDriver(new MockHandler, $sqsHandler);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('warehouse-orders', function (string $body, TransportContext $context) use (&$contexts): void {
            expect($body)->toBe(snsSqsMessageBody());
            $contexts[] = $context;
        });
    } catch (ConsumptionException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->failure)->toBe(ConsumptionFailure::ConsumerStopped);
    expect($contexts)->toHaveCount(1);
    expect($contexts[0]->driver)->toBe('snssqs');
    expect($contexts[0]->connectionName)->toBe('snssqs');
    expect($contexts[0]->topic)->toBe('orders');
    expect($contexts[0]->subscription)->toBe('warehouse-orders');
    expect($contexts[0]->headers)->toBe(['correlation-id' => 'A-42']);
    expect($contexts[0]->transportMessageId)->toBe('sqs-message-id');
    expect($contexts[0]->transportPublishedAt?->getTimestampMs())->toBe(1_784_112_188_417);
    expect($contexts[0]->redelivered)->toBeTrue();
    expect($contexts[0]->orderingKey)->toBe('order:42');
    expect($commands)->toBe([
        ['GetQueueUrl', null, '123456789012'],
        ['ReceiveMessage', $queueUrl, null],
        ['DeleteMessage', $queueUrl, null],
        ['ReceiveMessage', $queueUrl, null],
    ]);
});

test('leaves a delivery unsettled and propagates the same handoff failure', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        new Result(['Messages' => [sqsDelivery()]]),
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler);
    $failure = new RuntimeException('Laravel Queue handoff failed.');

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('warehouse-orders', static function () use ($failure): never {
            throw $failure;
        });
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
    expect($sqsHandler->getLastCommand()->getName())->toBe('ReceiveMessage');
});

test('reports settlement failure after a successful handoff', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $failure = new RuntimeException('DeleteMessage failed.');
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        new Result(['Messages' => [sqsDelivery()]]),
        $failure,
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('warehouse-orders', static function (): void {});
    } catch (ConsumptionException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->failure)->toBe(ConsumptionFailure::SettlementFailed);
    expect($caught?->getPrevious())->toBe($failure);
    expect($sqsHandler->getLastCommand()->getName())->toBe('DeleteMessage');
    expect($sqsHandler->getLastCommand()->get('QueueUrl'))->toBe($queueUrl);
});

/**
 * @return array<string, mixed>
 */
function snsSqsClientOptions(?MockHandler $handler = null): array
{
    $options = [
        'version' => 'latest',
        'region' => 'us-east-1',
        'endpoint' => 'http://localhost:4566',
        'credentials' => false,
        'md5' => false,
        'retries' => 0,
    ];

    if ($handler instanceof MockHandler) {
        $options['handler'] = $handler;
    }

    return $options;
}

function snsSqsDriver(
    MockHandler $snsHandler,
    ?MockHandler $sqsHandler = null,
    bool $fifo = true,
): SnsSqsDriver {
    config()->set('spoolrail.prefix', 'warehouse');

    $config = new ConnectionConfig('snssqs', [
        'region' => 'us-east-1',
        'account_id' => '123456789012',
        'fifo' => $fifo,
    ]);
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe(
        'orders',
        'warehouse-orders',
        RecordingMessageHandler::class,
    )->onConnection('snssqs');

    return new SnsSqsDriver(
        $config,
        new SnsClient(snsSqsClientOptions($snsHandler)),
        new SqsClient(snsSqsClientOptions($sqsHandler ?? new MockHandler)),
        Mockery::mock(CanManageTopology::class),
        app(OwnershipPrefix::class),
        $subscriptions,
    );
}

function snsSqsMessageBody(): string
{
    return json_encode([
        'id' => '01890a5d-ac96-774b-bcd0-48f622f3e798',
        'type' => 'order.created',
        'payload' => ['reference' => 'A-42'],
        'published_at' => '2026-07-15T14:23:08.417Z',
    ], JSON_THROW_ON_ERROR);
}

/**
 * @return array<string, mixed>
 */
function sqsDelivery(): array
{
    return [
        'MessageId' => 'sqs-message-id',
        'ReceiptHandle' => 'receipt-handle',
        'Body' => snsSqsMessageBody(),
        'MD5OfBody' => md5(snsSqsMessageBody()),
        'MD5OfMessageAttributes' => '85d1348856b5682dbd05ea3f9b6886f8',
        'Attributes' => [
            'SentTimestamp' => '1784112188417',
            'ApproximateReceiveCount' => '2',
            'MessageGroupId' => 'order:42',
        ],
        'MessageAttributes' => [
            'correlation-id' => [
                'DataType' => 'String',
                'StringValue' => 'A-42',
            ],
        ],
    ];
}
