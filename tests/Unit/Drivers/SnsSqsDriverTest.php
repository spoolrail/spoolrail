<?php

declare(strict_types=1);

use Aws\Exception\AwsException;
use Aws\Exception\CredentialsException;
use Aws\MockHandler;
use Aws\Result;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use GuzzleHttp\Psr7\Response;
use Ramsey\Uuid\Uuid;
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

test('reports SNS throttling as a retryable failure', function (): void {
    // --- Arrange ---
    $client = new SnsClient(snsSqsClientOptions());
    $failure = new AwsException(
        'Rate exceeded.',
        $client->getCommand('Publish'),
        [
            'response' => new Response(400),
            'code' => 'ThrottlingException',
        ],
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
    expect($caught?->outcome)->toBe(PublicationOutcome::NotSent);
    expect($caught?->getPrevious())->toBe($failure);
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

test('settles batched SQS deliveries individually after handoff', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $commands = [];
    $sqsHandler = new MockHandler([
        function ($command) use (&$commands, $queueUrl): Result {
            $commands[] = [
                $command->getName(),
                $command->get('QueueUrl'),
                $command->get('QueueOwnerAWSAccountId'),
                $command->get('MaxNumberOfMessages'),
            ];

            return new Result(['QueueUrl' => $queueUrl]);
        },
        function ($command) use (&$commands): Result {
            $commands[] = [$command->getName(), $command->get('QueueUrl'), null, $command->get('MaxNumberOfMessages')];

            return new Result(['Messages' => [
                sqsDelivery(),
                sqsDelivery('B-43', 'receipt-handle-2', 'sqs-message-id-2'),
            ]]);
        },
        function ($command) use (&$commands): Result {
            $commands[] = [$command->getName(), $command->get('QueueUrl'), null, null];

            return new Result;
        },
        function ($command) use (&$commands): Result {
            $commands[] = [$command->getName(), $command->get('QueueUrl'), null, null];

            return new Result;
        },
        function ($command) use (&$commands): never {
            $commands[] = [$command->getName(), $command->get('QueueUrl'), null, $command->get('MaxNumberOfMessages')];

            throw new RuntimeException('Stop after the settled batch.');
        },
    ]);
    $bodies = [];
    $contexts = [];
    $driver = snsSqsDriver(new MockHandler, $sqsHandler, receiveBatchSize: 2);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('warehouse-orders', function (string $body, TransportContext $context) use (&$bodies, &$contexts): void {
            $bodies[] = $body;
            $contexts[] = $context;
        });
    } catch (ConsumptionException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->failure)->toBe(ConsumptionFailure::ConsumerStopped);
    expect($bodies)->toBe([snsSqsMessageBody(), snsSqsMessageBody('B-43')]);
    expect($contexts)->toHaveCount(2);
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
        ['GetQueueUrl', null, '123456789012', null],
        ['ReceiveMessage', $queueUrl, null, 2],
        ['DeleteMessage', $queueUrl, null, null],
        ['DeleteMessage', $queueUrl, null, null],
        ['ReceiveMessage', $queueUrl, null, 2],
    ]);
});

test('reuses a FIFO receive identity for an SDK retry and refreshes it for the next receive', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $receiveRequestAttemptIds = [];
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        function ($command) use (&$receiveRequestAttemptIds): never {
            $receiveRequestAttemptIds[] = $command->get('ReceiveRequestAttemptId');

            throw new AwsException(
                'Service unavailable.',
                $command,
                ['response' => new Response(503)],
            );
        },
        function ($command) use (&$receiveRequestAttemptIds): Result {
            $receiveRequestAttemptIds[] = $command->get('ReceiveRequestAttemptId');

            return new Result(['Messages' => []]);
        },
        function ($command) use (&$receiveRequestAttemptIds): never {
            $receiveRequestAttemptIds[] = $command->get('ReceiveRequestAttemptId');

            throw new AwsException(
                'Forbidden.',
                $command,
                ['response' => new Response(403)],
            );
        },
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler, sqsRetries: 1);

    // --- Act ---
    try {
        $driver->consume('warehouse-orders', static function (): void {});
    } catch (ConsumptionException) {
    }

    // --- Assert ---
    expect($receiveRequestAttemptIds)->toHaveCount(3);
    expect(Uuid::isValid($receiveRequestAttemptIds[0]))->toBeTrue();
    expect($receiveRequestAttemptIds[1])->toBe($receiveRequestAttemptIds[0]);
    expect(Uuid::isValid($receiveRequestAttemptIds[2]))->toBeTrue();
    expect($receiveRequestAttemptIds[2])->not->toBe($receiveRequestAttemptIds[0]);
});

test('omits receive attempt identity for standard queues', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders';
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        new RuntimeException('Stop after the first receive.'),
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler, fifo: false, receiveBatchSize: 1);

    // --- Act ---
    try {
        $driver->consume('warehouse-orders', static function (): void {});
    } catch (ConsumptionException) {
    }

    // --- Assert ---
    expect($sqsHandler->getLastCommand()->toArray())
        ->not->toHaveKey('ReceiveRequestAttemptId');
    expect($sqsHandler->getLastCommand()->get('MaxNumberOfMessages'))->toBe(1);
});

test('settles only SQS deliveries whose handoffs complete', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        new Result(['Messages' => [
            sqsDelivery('A-41', 'receipt-handle-1'),
            sqsDelivery('A-42', 'receipt-handle-2'),
            sqsDelivery('A-43', 'receipt-handle-3'),
        ]]),
        new Result,
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler, receiveBatchSize: 3);
    $failure = new RuntimeException('Laravel Queue handoff failed.');
    $handedOffReferences = [];

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('warehouse-orders', static function (string $body) use (&$handedOffReferences, $failure): void {
            $handedOffReferences[] = json_decode($body, true, flags: JSON_THROW_ON_ERROR)['payload']['reference'];

            if (count($handedOffReferences) === 2) {
                throw $failure;
            }
        });
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
    expect($handedOffReferences)->toBe(['A-41', 'A-42']);
    expect($sqsHandler->getLastCommand()->getName())->toBe('DeleteMessage');
    expect($sqsHandler->getLastCommand()->get('ReceiptHandle'))->toBe('receipt-handle-1');
});

test('stops before the SQS batch tail when settlement fails', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $failure = new RuntimeException('DeleteMessage failed.');
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        new Result(['Messages' => [
            sqsDelivery('A-41', 'receipt-handle-1'),
            sqsDelivery('A-42', 'receipt-handle-2'),
            sqsDelivery('A-43', 'receipt-handle-3'),
        ]]),
        new Result,
        $failure,
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler, receiveBatchSize: 3);
    $handedOffReferences = [];

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('warehouse-orders', static function (string $body) use (&$handedOffReferences): void {
            $handedOffReferences[] = json_decode($body, true, flags: JSON_THROW_ON_ERROR)['payload']['reference'];
        });
    } catch (ConsumptionException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->failure)->toBe(ConsumptionFailure::SettlementFailed);
    expect($caught?->getPrevious())->toBe($failure);
    expect($handedOffReferences)->toBe(['A-41', 'A-42']);
    expect($sqsHandler->getLastCommand()->getName())->toBe('DeleteMessage');
    expect($sqsHandler->getLastCommand()->get('QueueUrl'))->toBe($queueUrl);
    expect($sqsHandler->getLastCommand()->get('ReceiptHandle'))->toBe('receipt-handle-2');
});

/**
 * @return array<string, mixed>
 */
function snsSqsClientOptions(?MockHandler $handler = null, int $retries = 0): array
{
    $options = [
        'version' => 'latest',
        'region' => 'us-east-1',
        'endpoint' => 'http://localhost:4566',
        'credentials' => false,
        'md5' => false,
        'retries' => $retries,
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
    int $sqsRetries = 0,
    int $receiveBatchSize = 10,
): SnsSqsDriver {
    config()->set('spoolrail.prefix', 'warehouse');

    $config = new ConnectionConfig('snssqs', [
        'region' => 'us-east-1',
        'account_id' => '123456789012',
        'fifo' => $fifo,
        'receive_batch_size' => $receiveBatchSize,
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
        new SqsClient(snsSqsClientOptions($sqsHandler ?? new MockHandler, $sqsRetries)),
        Mockery::mock(CanManageTopology::class),
        app(OwnershipPrefix::class),
        $subscriptions,
    );
}

function snsSqsMessageBody(string $reference = 'A-42'): string
{
    return json_encode([
        'id' => '01890a5d-ac96-774b-bcd0-48f622f3e798',
        'type' => 'order.created',
        'payload' => ['reference' => $reference],
        'published_at' => '2026-07-15T14:23:08.417Z',
    ], JSON_THROW_ON_ERROR);
}

/**
 * @return array<string, mixed>
 */
function sqsDelivery(
    string $reference = 'A-42',
    string $receiptHandle = 'receipt-handle',
    string $messageId = 'sqs-message-id',
): array {
    return [
        'MessageId' => $messageId,
        'ReceiptHandle' => $receiptHandle,
        'Body' => snsSqsMessageBody($reference),
        'MD5OfBody' => md5(snsSqsMessageBody($reference)),
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
