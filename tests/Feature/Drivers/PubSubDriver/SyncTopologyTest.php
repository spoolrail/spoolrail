<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\PubSubTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithPubSub;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithPubSub::class);

test('synchronizes ordered exactly-once fanout and preserves publication metadata', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');

    // --- Act ---
    $firstSync = $this->artisan('spoolrail:sync')->run();
    $published = Spoolrail::connection('pubsub')->publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
        ['correlation-id' => 'A-42'],
        orderingKey: 'order:42',
    );

    // --- Assert ---
    $warehouse = $this->pubSubSubscription('warehouse-orders');
    $billing = $this->pubSubSubscription('billing-orders');
    $warehouseMessage = $warehouse->pull(['maxMessages' => 1])[0];
    $billingMessage = $billing->pull(['maxMessages' => 1])[0];

    expect($firstSync)->toBe(0);
    expect($warehouse->info())->toMatchArray([
        'enableMessageOrdering' => true,
        'enableExactlyOnceDelivery' => true,
        'ackDeadlineSeconds' => 30,
    ]);
    expect($billing->info())->toMatchArray([
        'enableMessageOrdering' => true,
        'enableExactlyOnceDelivery' => true,
        'ackDeadlineSeconds' => 30,
    ]);
    expect((new MessageEnvelope)->decode($warehouseMessage->data()))->toEqual($published);
    expect((new MessageEnvelope)->decode($billingMessage->data()))->toEqual($published);
    expect($warehouseMessage->attributes())->toBe(['correlation-id' => 'A-42']);
    expect($warehouseMessage->orderingKey())->toBe('order:42');
});

test('reconciles the Pub/Sub acknowledgment deadline in place', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');
    $this->artisan('spoolrail:sync')->run();
    $subscriptionName = $this->pubSubSubscription('warehouse-orders')->name();

    config()->set('spoolrail.connections.pubsub.acknowledgment_deadline', 45);
    app(SpoolrailManager::class)->forgetConnection('pubsub');

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:sync')->run();

    // --- Assert ---
    $subscription = $this->pubSubSubscription('warehouse-orders');

    expect($exitCode)->toBe(0);
    expect($subscription->name())->toBe($subscriptionName);
    expect($subscription->reload()['ackDeadlineSeconds'])->toBe(45);
});

test('uses one ordered Pub/Sub lane when publications omit an application key', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');
    $this->artisan('spoolrail:sync')->run();

    // --- Act ---
    Spoolrail::connection('pubsub')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'first']),
    );
    Spoolrail::connection('pubsub')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'second']),
    );

    // --- Assert ---
    $subscription = $this->pubSubSubscription('warehouse-orders');
    $messages = [];

    for ($attempt = 0; $attempt < 3 && count($messages) < 2; $attempt++) {
        array_push(
            $messages,
            ...$subscription->pull(['maxMessages' => 2 - count($messages)]),
        );
    }

    $orderingKeys = [];

    foreach ($messages as $message) {
        $sequence = (new MessageEnvelope)->decode($message->data())->payload['sequence'];
        $orderingKeys[$sequence] = $message->orderingKey();
    }

    expect($orderingKeys)->toHaveKeys(['first', 'second']);
    expect($orderingKeys['first'])->not->toBeEmpty();
    expect($orderingKeys['second'])->toBe($orderingKeys['first']);
});

test('creates unordered at-least-once subscriptions without injecting a default key', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.pubsub.message_ordering', false);
    config()->set('spoolrail.connections.pubsub.exactly_once', false);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');
    $this->artisan('spoolrail:sync')->run();

    // --- Act ---
    Spoolrail::connection('pubsub')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'default']),
    );
    Spoolrail::connection('pubsub')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'caller-key']),
        orderingKey: 'order:42',
    );

    // --- Assert ---
    $subscription = $this->pubSubSubscription('warehouse-orders');
    $messages = [];

    for ($attempt = 0; $attempt < 3 && count($messages) < 2; $attempt++) {
        array_push(
            $messages,
            ...$subscription->pull(['maxMessages' => 2 - count($messages)]),
        );
    }

    $orderingKeys = [];

    foreach ($messages as $message) {
        $sequence = (new MessageEnvelope)->decode($message->data())->payload['sequence'];
        $orderingKeys[$sequence] = $message->orderingKey();
    }

    expect($subscription->info()['enableMessageOrdering'] ?? false)->toBeFalse();
    expect($subscription->info()['enableExactlyOnceDelivery'] ?? false)->toBeFalse();
    expect($orderingKeys)->toHaveCount(2);
    expect($orderingKeys['default'])->toBeEmpty();
    expect($orderingKeys['caller-key'])->toBe('order:42');
});

test('fails immutable ordering preflight before creating or updating resources', function (): void {
    // --- Arrange ---
    $this->pubsub->createTopic('orders');
    $this->pubsub->subscribe(
        config('spoolrail.prefix').'-warehouse-orders',
        'orders',
        [
            'enableMessageOrdering' => false,
            'enableExactlyOnceDelivery' => false,
        ],
    );

    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');
    Spoolrail::subscribe('returns', 'warehouse-returns', RecordingMessageHandler::class)
        ->onConnection('pubsub');

    // --- Act ---
    try {
        $this->artisan('spoolrail:sync')->run();
    } catch (TopologyPreflightException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->failuresByConnection['pubsub'] ?? null)
        ->toBeInstanceOf(PubSubTopologyException::class);
    expect($caught?->failuresByConnection['pubsub']->getMessage() ?? '')
        ->toContain('use a replacement subscription name');
    expect($this->pubSubSubscription('warehouse-orders')->info()['enableExactlyOnceDelivery'] ?? false)
        ->toBeFalse();
    expect($this->pubSubTopic('returns')->exists())->toBeFalse();
});

test('reconciles exactly-once delivery in place when ordering is compatible', function (): void {
    // --- Arrange ---
    $this->pubsub->createTopic('orders');
    $this->pubsub->subscribe(
        config('spoolrail.prefix').'-warehouse-orders',
        'orders',
        [
            'enableMessageOrdering' => true,
            'enableExactlyOnceDelivery' => false,
        ],
    );
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:sync')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->pubSubSubscription('warehouse-orders')->reload()['enableExactlyOnceDelivery'])
        ->toBeTrue();
});
