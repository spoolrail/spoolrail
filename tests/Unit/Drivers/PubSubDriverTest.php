<?php

declare(strict_types=1);

use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\PubSub\Message as PubSubMessage;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription as PubSubSubscription;
use Google\Cloud\PubSub\Topic;
use Google\Rpc\Code;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Drivers\PubSubDriver;
use Spoolrail\Spoolrail\Enums\ConsumptionFailure;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Spoolrail\Spoolrail\TransportContext;

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
    expect($caught?->outcome ?? null)->toBe(PublicationOutcome::Rejected);
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
    expect($caught?->outcome ?? null)->toBe(PublicationOutcome::Unknown);
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
    expect($caught?->outcome ?? null)->toBe(PublicationOutcome::Unknown);
    expect($caught?->getPrevious()?->getMessage())->toContain('returned no message ID');
});

test('settles batched Pub/Sub deliveries individually after handoff', function (): void {
    // --- Arrange ---
    $first = pubSubDelivery();
    $second = pubSubDelivery('B-43', 'ack-id-2', 'pubsub-message-id-2');
    $events = [];
    $subscription = Mockery::mock(PubSubSubscription::class);
    $subscription->expects('pull')->once()->with(['maxMessages' => 2])->andReturn([$first, $second]);
    $subscription->expects('acknowledge')
        ->twice()
        ->withArgs(
            static function (PubSubMessage $settled, array $options) use (&$events): bool {
                $reference = json_decode(
                    $settled->data(),
                    true,
                    flags: JSON_THROW_ON_ERROR,
                )['payload']['reference'];
                $followsHandoff = end($events) === "handoff:$reference";
                $events[] = "settle:$reference";

                return $followsHandoff && $options === ['returnFailures' => true];
            },
        )
        ->andReturn([]);
    $pullFailure = new RuntimeException('Stop after the settled batch.');
    $subscription->expects('pull')->once()->with(['maxMessages' => 2])->andThrow($pullFailure);

    $consumer = Mockery::mock(PubSubClient::class);
    $consumer->expects('subscription')
        ->with('warehouse-warehouse-orders')
        ->andReturn($subscription);
    $bodies = [];
    $contexts = [];
    $driver = pubSubDriver(consumer: $consumer, receiveBatchSize: 2);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('warehouse-orders', function (string $body, TransportContext $context) use (&$bodies, &$contexts, &$events): void {
            $bodies[] = $body;
            $contexts[] = $context;
            $reference = json_decode($body, true, flags: JSON_THROW_ON_ERROR)['payload']['reference'];
            $events[] = "handoff:$reference";
        });
    } catch (ConsumptionException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->failure ?? null)->toBe(ConsumptionFailure::ConsumerStopped);
    expect($caught?->getPrevious())->toBe($pullFailure);
    expect($bodies)->toBe([pubSubMessageBody(), pubSubMessageBody('B-43')]);
    expect($events)->toBe(['handoff:A-42', 'settle:A-42', 'handoff:B-43', 'settle:B-43']);
    expect($contexts)->toHaveCount(2);
    expect($contexts[0]->driver)->toBe('pubsub');
    expect($contexts[0]->connectionName)->toBe('pubsub');
    expect($contexts[0]->topic)->toBe('orders');
    expect($contexts[0]->subscription)->toBe('warehouse-orders');
    expect($contexts[0]->headers)->toBe(['correlation-id' => 'A-42']);
    expect($contexts[0]->transportMessageId)->toBe('pubsub-message-id');
    expect($contexts[0]->transportPublishedAt?->format('Y-m-d H:i:s.v'))->toBe('2026-07-15 14:23:08.417');
    expect($contexts[0]->redelivered)->toBeTrue();
    expect($contexts[0]->orderingKey)->toBe('order:42');
});

test('leaves optional delivery context unknown when Pub/Sub does not report it', function (): void {
    // --- Arrange ---
    $message = new PubSubMessage([
        'data' => pubSubMessageBody(),
        'messageId' => 'pubsub-message-id',
        'publishTime' => '2026-07-15T14:23:08.417Z',
    ], [
        'ackId' => 'ack-id',
    ]);
    $subscription = Mockery::mock(PubSubSubscription::class);
    $subscription->expects('pull')->once()->with(['maxMessages' => 1])->andReturn([$message]);
    $subscription->expects('acknowledge')->once()->andReturn([]);
    $subscription->expects('pull')->once()->andThrow(new RuntimeException('Stop after one delivery.'));
    $consumer = Mockery::mock(PubSubClient::class);
    $consumer->expects('subscription')->andReturn($subscription);
    $contexts = [];
    $driver = pubSubDriver(consumer: $consumer, receiveBatchSize: 1);

    // --- Act ---
    try {
        $driver->consume('warehouse-orders', static function (string $body, TransportContext $context) use (&$contexts): void {
            $contexts[] = $context;
        });
    } catch (ConsumptionException) {
    }

    // --- Assert ---
    expect($contexts)->toHaveCount(1);
    expect($contexts[0]->headers)->toBe([]);
    expect($contexts[0]->redelivered)->toBeNull();
    expect($contexts[0]->orderingKey)->toBeNull();
});

test('settles only Pub/Sub deliveries whose handoffs complete', function (): void {
    // --- Arrange ---
    $first = pubSubDelivery('A-41', 'ack-id-1', 'pubsub-message-id-1');
    $second = pubSubDelivery('A-42', 'ack-id-2', 'pubsub-message-id-2');
    $third = pubSubDelivery('A-43', 'ack-id-3', 'pubsub-message-id-3');
    $subscription = Mockery::mock(PubSubSubscription::class);
    $subscription->expects('pull')->once()->with(['maxMessages' => 3])->andReturn([$first, $second, $third]);
    $subscription->expects('acknowledge')
        ->once()
        ->with($first, ['returnFailures' => true])
        ->andReturn([]);
    $consumer = Mockery::mock(PubSubClient::class);
    $consumer->expects('subscription')->andReturn($subscription);
    $driver = pubSubDriver(consumer: $consumer, receiveBatchSize: 3);
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
    expect($caught ?? null)->toBe($failure);
    expect($handedOffReferences)->toBe(['A-41', 'A-42']);
});

test('stops before the Pub/Sub batch tail when settlement fails', function (): void {
    // --- Arrange ---
    $first = pubSubDelivery('A-41', 'ack-id-1', 'pubsub-message-id-1');
    $second = pubSubDelivery('A-42', 'ack-id-2', 'pubsub-message-id-2');
    $third = pubSubDelivery('A-43', 'ack-id-3', 'pubsub-message-id-3');
    $failure = new RuntimeException('Acknowledge request failed.');
    $subscription = Mockery::mock(PubSubSubscription::class);
    $subscription->expects('pull')->once()->with(['maxMessages' => 3])->andReturn([$first, $second, $third]);
    $subscription->expects('acknowledge')
        ->once()
        ->with($first, ['returnFailures' => true])
        ->andReturn([])
        ->ordered();
    $subscription->expects('acknowledge')
        ->once()
        ->with($second, ['returnFailures' => true])
        ->andThrow($failure)
        ->ordered();
    $consumer = Mockery::mock(PubSubClient::class);
    $consumer->expects('subscription')->andReturn($subscription);
    $driver = pubSubDriver(consumer: $consumer, receiveBatchSize: 3);
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
    expect($caught?->failure ?? null)->toBe(ConsumptionFailure::SettlementFailed);
    expect($caught?->getPrevious())->toBe($failure);
    expect($handedOffReferences)->toBe(['A-41', 'A-42']);
});

test('reports a failed exactly-once acknowledgment as settlement failure', function (): void {
    // --- Arrange ---
    $message = pubSubDelivery();
    $subscription = Mockery::mock(PubSubSubscription::class);
    $subscription->expects('pull')->once()->andReturn([$message]);
    $subscription->expects('acknowledge')->once()->andReturn([$message]);
    $consumer = Mockery::mock(PubSubClient::class);
    $consumer->expects('subscription')->andReturn($subscription);
    $driver = pubSubDriver(consumer: $consumer);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('warehouse-orders', static function (): void {});
    } catch (ConsumptionException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->failure ?? null)->toBe(ConsumptionFailure::SettlementFailed);
    expect($caught?->getPrevious()?->getMessage())->toContain('acknowledgment failed');
});

function pubSubDriver(
    ?PubSubClient $publisher = null,
    ?PubSubClient $consumer = null,
    int $receiveBatchSize = 10,
): PubSubDriver {
    config()->set('spoolrail.prefix', 'warehouse');

    $config = new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail',
        'receive_batch_size' => $receiveBatchSize,
    ]);
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe(
        'orders',
        'warehouse-orders',
        RecordingMessageHandler::class,
    )->onConnection('pubsub');

    return new PubSubDriver(
        $config,
        $publisher ?? Mockery::mock(PubSubClient::class),
        $consumer ?? Mockery::mock(PubSubClient::class),
        Mockery::mock(CanManageTopology::class),
        app(OwnershipPrefix::class),
        $subscriptions,
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

function pubSubDelivery(
    string $reference = 'A-42',
    string $ackId = 'ack-id',
    string $messageId = 'pubsub-message-id',
): PubSubMessage {
    return new PubSubMessage([
        'data' => pubSubMessageBody($reference),
        'attributes' => ['correlation-id' => 'A-42'],
        'messageId' => $messageId,
        'publishTime' => '2026-07-15T14:23:08.417Z',
        'orderingKey' => 'order:42',
    ], [
        'ackId' => $ackId,
        'deliveryAttempt' => 2,
    ]);
}
