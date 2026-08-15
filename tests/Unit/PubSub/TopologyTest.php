<?php

declare(strict_types=1);

use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription as PubSubSubscription;
use Google\Cloud\PubSub\Topic;
use Google\Rpc\Code;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\PubSubTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig;
use Spoolrail\Spoolrail\PubSub\Topology;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

test('rejects a same-named subscription attached to another topic', function (): void {
    // --- Arrange ---
    $topic = Mockery::mock(Topic::class);
    $topic->expects('info')->once()->andReturn([
        'name' => 'projects/spoolrail/topics/orders',
    ]);

    $subscription = Mockery::mock(PubSubSubscription::class);
    $subscription->expects('info')->once()->andReturn([
        'name' => 'projects/spoolrail/subscriptions/warehouse-warehouse-orders',
        'topic' => 'projects/spoolrail/topics/returns',
        'enableMessageOrdering' => true,
        'enableExactlyOnceDelivery' => true,
    ]);

    $client = Mockery::mock(PubSubClient::class);
    $client->expects('topic')->once()->andReturn($topic);
    $client->expects('subscription')->once()->andReturn($subscription);

    // --- Act & Assert ---
    expect(fn (): TopologyPlan => pubSubTopology($client)->planSync(
        [pubSubSubscription()],
        'warehouse',
    ))->toThrow(
        PubSubTopologyException::class,
        'must belong to topic [projects/spoolrail/topics/orders]',
    );
});

test('rejects a same-named subscription configured for push delivery', function (): void {
    // --- Arrange ---
    $topic = Mockery::mock(Topic::class);
    $topic->expects('info')->once()->andReturn([
        'name' => 'projects/spoolrail/topics/orders',
    ]);

    $subscription = Mockery::mock(PubSubSubscription::class);
    $subscription->expects('info')->once()->andReturn([
        'name' => 'projects/spoolrail/subscriptions/warehouse-warehouse-orders',
        'topic' => 'projects/spoolrail/topics/orders',
        'pushConfig' => ['pushEndpoint' => 'https://example.com/pubsub'],
        'enableMessageOrdering' => true,
        'enableExactlyOnceDelivery' => true,
    ]);

    $client = Mockery::mock(PubSubClient::class);
    $client->expects('topic')->once()->andReturn($topic);
    $client->expects('subscription')->once()->andReturn($subscription);

    // --- Act & Assert ---
    expect(fn (): TopologyPlan => pubSubTopology($client)->planSync(
        [pubSubSubscription()],
        'warehouse',
    ))->toThrow(
        PubSubTopologyException::class,
        'must use pull delivery',
    );
});

test('requests a fresh synchronization after a transient discovery failure', function (): void {
    // --- Arrange ---
    $failure = new ServiceException('Service unavailable.', Code::UNAVAILABLE);
    $topic = Mockery::mock(Topic::class);
    $topic->expects('info')->once()->andThrow($failure);
    $client = Mockery::mock(PubSubClient::class);
    $client->expects('topic')->once()->with('orders')->andReturn($topic);
    $topology = pubSubTopology($client);

    // --- Act ---
    $caught = null;

    try {
        $topology->planSync([pubSubSubscription()], 'warehouse');
    } catch (TopologySyncRequiresRetryException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->getPrevious())->toBeInstanceOf(PubSubTopologyException::class);
    expect($caught?->getPrevious()?->getPrevious())->toBe($failure);
});

test('finds only undeclared application-owned subscriptions', function (): void {
    // --- Arrange ---
    $current = Mockery::mock(PubSubSubscription::class);
    $current->expects('name')->andReturn('projects/spoolrail/subscriptions/warehouse-current-orders');
    $undeclared = Mockery::mock(PubSubSubscription::class);
    $undeclared->expects('name')->andReturn('projects/spoolrail/subscriptions/warehouse-old-orders');
    $foreign = Mockery::mock(PubSubSubscription::class);
    $foreign->expects('name')->andReturn('projects/spoolrail/subscriptions/billing-orders');
    $client = Mockery::mock(PubSubClient::class);
    $client->expects('subscriptions')->once()->andReturn([$current, $undeclared, $foreign]);

    // --- Act ---
    $resources = pubSubTopology($client)->undeclaredSubscriptionResourceNames(
        [pubSubSubscription('orders', 'current-orders')],
        'warehouse',
    );

    // --- Assert ---
    expect($resources)->toBe(['warehouse-old-orders']);
});

function pubSubTopology(PubSubClient $client): Topology
{
    return new Topology(
        new ConnectionConfig('pubsub', [
            'project_id' => 'spoolrail',
        ]),
        $client,
    );
}

function pubSubSubscription(
    string $topic = 'orders',
    string $name = 'warehouse-orders',
): Subscription {
    return new Subscription(
        $topic,
        $name,
        RecordingMessageHandler::class,
        static function (): void {},
    );
}
