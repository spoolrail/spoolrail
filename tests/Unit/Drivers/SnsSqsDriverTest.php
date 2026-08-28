<?php

declare(strict_types=1);

use Aws\CommandInterface;
use Aws\Exception\AwsException;
use Aws\Exception\CredentialsException;
use Aws\MockHandler;
use Aws\Result;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use GuzzleHttp\Handler\CurlMultiHandler;
use GuzzleHttp\Psr7\Response;
use Ramsey\Uuid\Uuid;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Drivers\SnsSqsDriver;
use Spoolrail\Spoolrail\Enums\ConsumptionFailure;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\SnsSqs\ConnectionConfig;
use Spoolrail\Spoolrail\SnsSqs\Receipt;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

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

test('receives a native SQS batch through a long poll and settles each delivery independently', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        new Result(['Messages' => [
            sqsDelivery(),
            sqsDelivery('B-43', 'receipt-handle-2', 'sqs-message-id-2'),
        ]]),
        new Result,
        new Result,
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler, receiveBatchSize: 2);
    $deliveries = [];

    // --- Act ---
    $driver->receive('warehouse-orders', function (array $received) use (&$deliveries): void {
        $deliveries = $received;
    }, static function (): void {});
    $driver->waitForConsumerIo();
    $waitTimeSeconds = $sqsHandler->getLastCommand()->get('WaitTimeSeconds');

    $acknowledged = 0;
    foreach ($deliveries as $delivery) {
        $driver->acknowledge(
            $delivery,
            function () use (&$acknowledged): void {
                $acknowledged++;
            },
            static function (): void {},
        );
    }
    $driver->waitForConsumerIo();

    // --- Assert ---
    expect($deliveries)->toHaveCount(2);
    expect($waitTimeSeconds)->toBe(20);
    expect($deliveries[0]->body)->toBe(snsSqsMessageBody());
    expect($deliveries[0]->headers)->toBe(['correlation-id' => 'A-42']);
    expect($deliveries[0]->transportMessageId)->toBe('sqs-message-id');
    expect($deliveries[0]->transportPublishedAt?->getTimestampMs())->toBe(1_784_112_188_417);
    expect($deliveries[0]->redelivered)->toBeTrue();
    expect($deliveries[0]->orderingKey)->toBe('order:42');
    expect($acknowledged)->toBe(2);
    expect($sqsHandler->getLastCommand()->getName())->toBe('DeleteMessage');
    expect($sqsHandler->getLastCommand()->get('ReceiptHandle'))->toBe('receipt-handle-2');
});

test('keeps a sibling SQS receive usable after one subscription returns an invalid delivery', function (): void {
    // --- Arrange ---
    $warehouseQueueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $billingQueueUrl = 'http://localhost:4566/123456789012/billing-orders.fifo';
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $warehouseQueueUrl]),
        new Result(['Messages' => [['ReceiptHandle' => 'invalid']]]),
        new Result(['QueueUrl' => $billingQueueUrl]),
        new Result(['Messages' => [sqsDelivery('B-43')]]),
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler);
    $warehouseFailure = null;
    $billingDeliveries = [];

    // --- Act ---
    $driver->receive(
        'warehouse-orders',
        static function (): void {},
        function (Throwable $exception) use (&$warehouseFailure): void {
            $warehouseFailure = $exception;
        },
    );
    $driver->receive(
        'billing-orders',
        function (array $deliveries) use (&$billingDeliveries): void {
            $billingDeliveries = $deliveries;
        },
        static function (): void {},
    );

    for ($tick = 0; $tick < 5 && (! $warehouseFailure instanceof Throwable || $billingDeliveries === []); $tick++) {
        $driver->waitForConsumerIo();
    }

    // --- Assert ---
    expect($warehouseFailure)->toBeInstanceOf(ConsumptionException::class);
    expect($warehouseFailure->failure)->toBe(ConsumptionFailure::ConsumerStopped);
    expect($billingDeliveries)->toHaveCount(1);
    expect($billingDeliveries[0]->body)->toBe(snsSqsMessageBody('B-43'));
});

test('uses a fresh FIFO receive identity for each logical receive attempt', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $receiveRequestAttemptIds = [];
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        function ($command) use (&$receiveRequestAttemptIds): Result {
            $receiveRequestAttemptIds[] = $command->get('ReceiveRequestAttemptId');

            return new Result(['Messages' => []]);
        },
        function ($command) use (&$receiveRequestAttemptIds): Result {
            $receiveRequestAttemptIds[] = $command->get('ReceiveRequestAttemptId');

            return new Result(['Messages' => []]);
        },
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler);

    // --- Act ---
    $driver->receive('warehouse-orders', static function (): void {}, static function (): void {});
    $driver->waitForConsumerIo();
    $driver->receive('warehouse-orders', static function (): void {}, static function (): void {});
    $driver->waitForConsumerIo();

    // --- Assert ---
    expect($receiveRequestAttemptIds)->toHaveCount(2);
    expect(Uuid::isValid($receiveRequestAttemptIds[0]))->toBeTrue();
    expect(Uuid::isValid($receiveRequestAttemptIds[1]))->toBeTrue();
    expect($receiveRequestAttemptIds[1])->not->toBe($receiveRequestAttemptIds[0]);
});

test('keeps one FIFO receive identity across retries of a logical attempt', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $receiveRequestAttemptIds = [];
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        function (CommandInterface $command) use (&$receiveRequestAttemptIds): AwsException {
            $receiveRequestAttemptIds[] = $command->get('ReceiveRequestAttemptId');

            return new AwsException(
                'SQS is temporarily unavailable.',
                $command,
                [
                    'response' => new Response(503),
                    'code' => 'ServiceUnavailable',
                ],
            );
        },
        function (CommandInterface $command) use (&$receiveRequestAttemptIds): Result {
            $receiveRequestAttemptIds[] = $command->get('ReceiveRequestAttemptId');

            return new Result(['Messages' => []]);
        },
    ]);
    $driver = snsSqsDriver(
        new MockHandler,
        $sqsHandler,
        sqsRetries: 1,
    );
    $received = false;
    $failure = null;

    // --- Act ---
    $driver->receive(
        'warehouse-orders',
        function () use (&$received): void {
            $received = true;
        },
        function (Throwable $exception) use (&$failure): void {
            $failure = $exception;
        },
    );

    for ($tick = 0; $tick < 20 && ! $received && ! $failure instanceof Throwable; $tick++) {
        $driver->waitForConsumerIo();
    }

    // --- Assert ---
    expect($failure)->toBeNull();
    expect($received)->toBeTrue();
    expect($receiveRequestAttemptIds)->toHaveCount(2);
    expect($receiveRequestAttemptIds[0])->toBeString();
    expect($receiveRequestAttemptIds[1])->toBe($receiveRequestAttemptIds[0]);
});

test('releases an SQS delivery by making it immediately visible', function (): void {
    // --- Arrange ---
    $queueUrl = 'http://localhost:4566/123456789012/warehouse-orders.fifo';
    $sqsHandler = new MockHandler([
        new Result(['QueueUrl' => $queueUrl]),
        new Result(['Messages' => [sqsDelivery()]]),
        new Result,
    ]);
    $driver = snsSqsDriver(new MockHandler, $sqsHandler);
    $delivery = null;
    $driver->receive('warehouse-orders', function (array $deliveries) use (&$delivery): void {
        $delivery = $deliveries[0];
    }, static function (): void {});
    $driver->waitForConsumerIo();

    // --- Act ---
    $released = false;
    $driver->release(
        $delivery,
        function () use (&$released): void {
            $released = true;
        },
        static function (): void {},
    );
    $driver->waitForConsumerIo();

    // --- Assert ---
    expect($released)->toBeTrue();
    expect($sqsHandler->getLastCommand()->getName())->toBe('ChangeMessageVisibility');
    expect($sqsHandler->getLastCommand()->get('ReceiptHandle'))->toBe('receipt-handle');
    expect($sqsHandler->getLastCommand()->get('VisibilityTimeout'))->toBe(0);
});

test('reports independent SQS settlement failures through their delivery callbacks', function (): void {
    // --- Arrange ---
    $client = new SqsClient(snsSqsClientOptions());
    $acknowledgmentFailure = new AwsException(
        'DeleteMessage failed.',
        $client->getCommand('DeleteMessage'),
        ['response' => new Response(503)],
    );
    $releaseFailure = new AwsException(
        'ChangeMessageVisibility failed.',
        $client->getCommand('ChangeMessageVisibility'),
        ['response' => new Response(503)],
    );
    $driver = snsSqsDriver(
        new MockHandler,
        new MockHandler([$acknowledgmentFailure, $releaseFailure]),
    );
    $delivery = new Delivery(
        'body',
        new Receipt('https://sqs.example/warehouse-orders', 'receipt-handle'),
    );
    $completed = 0;
    $failures = [];

    // --- Act ---
    $driver->acknowledge(
        $delivery,
        function () use (&$completed): void {
            $completed++;
        },
        function (Throwable $exception) use (&$failures): void {
            $failures[] = $exception;
        },
    );
    $driver->release(
        $delivery,
        function () use (&$completed): void {
            $completed++;
        },
        function (Throwable $exception) use (&$failures): void {
            $failures[] = $exception;
        },
    );
    $driver->waitForConsumerIo();

    // --- Assert ---
    expect($completed)->toBe(0);
    expect($failures)->toHaveCount(2);
    expect($failures[0])->toBeInstanceOf(ConsumptionException::class);
    expect($failures[0]->failure)->toBe(ConsumptionFailure::SettlementFailed);
    expect($failures[0]->getPrevious())->toBe($acknowledgmentFailure);
    expect($failures[1])->toBeInstanceOf(ConsumptionException::class);
    expect($failures[1]->failure)->toBe(ConsumptionFailure::SettlementFailed);
    expect($failures[1]->getPrevious())->toBe($releaseFailure);
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

    return new SnsSqsDriver(
        $config,
        new SnsClient(snsSqsClientOptions($snsHandler)),
        new SqsClient(snsSqsClientOptions($sqsHandler ?? new MockHandler, $sqsRetries)),
        Mockery::mock(CanManageTopology::class),
        app(OwnershipPrefix::class),
        new CurlMultiHandler(['select_timeout' => 0.001]),
        1,
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
