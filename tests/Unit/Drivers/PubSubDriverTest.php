<?php

declare(strict_types=1);

use Google\ApiCore\InsecureCredentialsWrapper;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use Google\Cloud\PubSub\V1\Client\SubscriberClient;
use Google\Rpc\Code;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Handler\CurlMultiHandler;
use GuzzleHttp\Handler\MockHandler as GuzzleMockHandler;
use GuzzleHttp\Promise\RejectedPromise;
use GuzzleHttp\Psr7\Response;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Drivers\PubSubDriver;
use Spoolrail\Spoolrail\Enums\ConsumptionFailure;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

test('reports an explicit Pub/Sub refusal as rejected', function (): void {
    // --- Arrange ---
    $failure = new ServiceException('Permission denied.', Code::PERMISSION_DENIED);
    $topic = Mockery::mock(Topic::class);
    $topic->expects('publish')->once()->andThrow($failure);
    $publisher = Mockery::mock(PubSubClient::class);
    $publisher->expects('topic')->with('orders')->andReturn($topic);
    $driver = pubSubDriver($publisher);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', pubSubMessageBody(), []);
    } catch (PublicationException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->outcome)->toBe(PublicationOutcome::Rejected);
    expect($caught?->getPrevious())->toBe($failure);
});

test('reports an uncertain Pub/Sub transport failure as unknown', function (): void {
    // --- Arrange ---
    $failure = new ServiceException('Service unavailable.', Code::UNAVAILABLE);
    $topic = Mockery::mock(Topic::class);
    $topic->expects('publish')->once()->andThrow($failure);
    $publisher = Mockery::mock(PubSubClient::class);
    $publisher->expects('topic')->with('orders')->andReturn($topic);
    $driver = pubSubDriver($publisher);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', pubSubMessageBody(), []);
    } catch (PublicationException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->outcome)->toBe(PublicationOutcome::Unknown);
    expect($caught?->getPrevious())->toBe($failure);
});

test('reports an unknown outcome when Pub/Sub returns no message ID', function (): void {
    // --- Arrange ---
    $topic = Mockery::mock(Topic::class);
    $topic->expects('publish')->once()->andReturn(['messageIds' => []]);
    $publisher = Mockery::mock(PubSubClient::class);
    $publisher->expects('topic')->with('orders')->andReturn($topic);
    $driver = pubSubDriver($publisher);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', pubSubMessageBody(), []);
    } catch (PublicationException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->outcome)->toBe(PublicationOutcome::Unknown);
    expect($caught?->getPrevious()?->getMessage())->toContain('returned no message ID');
});

test('receives a native Pub/Sub batch and acknowledges one delivery', function (): void {
    // --- Arrange ---
    $handler = new GuzzleMockHandler([
        pubSubPullResponse(),
        new Response(200, ['Content-Type' => 'application/json'], '{}'),
    ]);
    $driver = pubSubDriver(subscriberHandler: $handler, receiveBatchSize: 2);
    $deliveries = [];

    // --- Act ---
    $driver->receive('warehouse-orders', function (array $received) use (&$deliveries): void {
        $deliveries = $received;
    }, static function (): void {});
    $driver->waitForConsumerIo();

    $acknowledged = false;
    $driver->acknowledge(
        $deliveries[0],
        function () use (&$acknowledged): void {
            $acknowledged = true;
        },
        static function (): void {},
    );
    $driver->waitForConsumerIo();

    // --- Assert ---
    expect($deliveries)->toHaveCount(2);
    expect($deliveries[0]->body)->toBe(pubSubMessageBody());
    expect($deliveries[0]->headers)->toBe(['correlation-id' => 'A-42']);
    expect($deliveries[0]->transportMessageId)->toBe('pubsub-message-id');
    expect($deliveries[0]->transportPublishedAt?->format('Y-m-d H:i:s.v'))->toBe('2026-07-15 14:23:08.417');
    expect($deliveries[0]->redelivered)->toBeTrue();
    expect($deliveries[0]->orderingKey)->toBe('order:42');
    expect($acknowledged)->toBeTrue();
    expect((string) $handler->getLastRequest()?->getBody())
        ->toContain('"ackIds":["ack-id"]');
});

test('keeps a sibling Pub/Sub receive usable after one subscription returns an invalid delivery', function (): void {
    // --- Arrange ---
    $invalidResponse = new Response(
        200,
        ['Content-Type' => 'application/json'],
        json_encode([
            'receivedMessages' => [[
                'message' => [
                    'data' => base64_encode(pubSubMessageBody()),
                    'messageId' => 'invalid-message',
                ],
            ]],
        ], JSON_THROW_ON_ERROR),
    );
    $handler = new GuzzleMockHandler([
        $invalidResponse,
        pubSubPullResponse(1),
    ]);
    $driver = pubSubDriver(subscriberHandler: $handler);
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
    expect($billingDeliveries[0]->body)->toBe(pubSubMessageBody());
});

test('reports a permanent exactly-once acknowledgment failure for its delivery', function (): void {
    // --- Arrange ---
    $handler = new GuzzleMockHandler([
        pubSubPullResponse(1),
        static function ($request): RejectedPromise {
            $response = new Response(400, ['Content-Type' => 'application/json'], json_encode([
                'error' => [
                    'code' => 400,
                    'message' => 'The acknowledgment ID has expired.',
                    'status' => 'INVALID_ARGUMENT',
                ],
            ], JSON_THROW_ON_ERROR));

            return new RejectedPromise(new ClientException(
                'The acknowledgment ID has expired.',
                $request,
                $response,
            ));
        },
    ]);
    $driver = pubSubDriver(subscriberHandler: $handler);
    $delivery = null;
    $driver->receive('warehouse-orders', function (array $deliveries) use (&$delivery): void {
        $delivery = $deliveries[0];
    }, static function (): void {});
    $driver->waitForConsumerIo();

    // --- Act ---
    $failure = null;
    $driver->acknowledge(
        $delivery,
        static function (): void {},
        function (Throwable $exception) use (&$failure): void {
            $failure = $exception;
        },
    );
    for ($tick = 0; $tick < 5 && ! $failure instanceof Throwable; $tick++) {
        $driver->waitForConsumerIo();
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(ConsumptionException::class);
    expect($failure->failure)->toBe(ConsumptionFailure::SettlementFailed);
    expect($failure->getPrevious()?->getCode())->toBe(Code::INVALID_ARGUMENT);
});

test('retries a transient exactly-once acknowledgment without blocking another receive', function (): void {
    // --- Arrange ---
    $handler = new GuzzleMockHandler([
        pubSubPullResponse(1),
        static function ($request): RejectedPromise {
            $response = new Response(503, ['Content-Type' => 'application/json'], json_encode([
                'error' => [
                    'code' => 503,
                    'message' => 'Pub/Sub is temporarily unavailable.',
                    'status' => 'UNAVAILABLE',
                ],
            ], JSON_THROW_ON_ERROR));

            return new RejectedPromise(new ClientException(
                'Pub/Sub is temporarily unavailable.',
                $request,
                $response,
            ));
        },
        pubSubPullResponse(1),
        new Response(200, ['Content-Type' => 'application/json'], '{}'),
    ]);
    $driver = pubSubDriver(subscriberHandler: $handler);
    $warehouse = null;
    $billing = null;
    $acknowledged = false;
    $failure = null;
    $driver->receive('warehouse-orders', function (array $deliveries) use (&$warehouse): void {
        $warehouse = $deliveries[0];
    }, static function (): void {});
    $driver->waitForConsumerIo();

    // --- Act ---
    $driver->acknowledge(
        $warehouse,
        function () use (&$acknowledged): void {
            $acknowledged = true;
        },
        function (Throwable $exception) use (&$failure): void {
            $failure = $exception;
        },
    );
    $driver->waitForConsumerIo();
    $driver->receive('billing-orders', function (array $deliveries) use (&$billing): void {
        $billing = $deliveries[0];
    }, static function (): void {});
    $driver->waitForConsumerIo();

    // --- Assert ---
    expect($billing)->toBeInstanceOf(Delivery::class);
    expect($acknowledged)->toBeFalse();
    expect($failure)->toBeNull();

    usleep(1_100_000);

    for ($tick = 0; $tick < 5 && ! $acknowledged && ! $failure instanceof Throwable; $tick++) {
        $driver->waitForConsumerIo();
    }

    expect($acknowledged)->toBeTrue();
    expect($failure)->toBeNull();
    $driver->close();
});

test('releases a Pub/Sub delivery through its acknowledgment deadline', function (): void {
    // --- Arrange ---
    $handler = new GuzzleMockHandler([
        pubSubPullResponse(1),
        new Response(200, ['Content-Type' => 'application/json'], '{}'),
    ]);
    $driver = pubSubDriver(subscriberHandler: $handler);
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
    expect((string) $handler->getLastRequest()?->getUri())
        ->toContain(':modifyAckDeadline');
    expect((string) $handler->getLastRequest()?->getBody())
        ->toContain('"ackIds":["ack-id"]');
});

function pubSubDriver(
    ?PubSubClient $publisher = null,
    ?GuzzleMockHandler $subscriberHandler = null,
    int $receiveBatchSize = 10,
): PubSubDriver {
    config()->set('spoolrail.prefix', 'warehouse');

    $config = new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail',
        'receive_batch_size' => $receiveBatchSize,
    ]);
    $handler = $subscriberHandler ?? new GuzzleMockHandler;
    $options = $config->subscriberClientOptions($handler);
    $options['credentials'] = new InsecureCredentialsWrapper;

    return new PubSubDriver(
        $config,
        $publisher ?? Mockery::mock(PubSubClient::class),
        new SubscriberClient($options),
        Mockery::mock(CanManageTopology::class),
        app(OwnershipPrefix::class),
        new CurlMultiHandler(['select_timeout' => 0.001]),
        1,
    );
}

function pubSubMessageBody(string $reference = 'A-42'): string
{
    return json_encode([
        'id' => '01890a5d-ac96-774b-bcd0-48f622f3e798',
        'type' => 'order.created',
        'payload' => ['reference' => $reference],
        'published_at' => '2026-07-15T14:23:08.417Z',
    ], JSON_THROW_ON_ERROR);
}

function pubSubPullResponse(int $messages = 2): Response
{
    $received = [];

    for ($index = 0; $index < $messages; $index++) {
        $suffix = $index === 0 ? '' : '-'.($index + 1);
        $received[] = [
            'ackId' => "ack-id$suffix",
            'deliveryAttempt' => 2,
            'message' => [
                'data' => base64_encode(pubSubMessageBody($index === 0 ? 'A-42' : 'B-43')),
                'attributes' => ['correlation-id' => 'A-42'],
                'messageId' => "pubsub-message-id$suffix",
                'publishTime' => '2026-07-15T14:23:08.417Z',
                'orderingKey' => 'order:42',
            ],
        ];
    }

    return new Response(200, ['Content-Type' => 'application/json'], json_encode([
        'receivedMessages' => $received,
    ], JSON_THROW_ON_ERROR));
}
