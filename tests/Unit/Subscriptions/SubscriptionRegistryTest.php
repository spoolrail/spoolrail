<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

test('rejects a duplicate name without replacing the registered subscription', function (): void {
    $subscriptions = new SubscriptionRegistry;
    $registered = $subscriptions->subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);

    expect(fn (): Subscription => $subscriptions->subscribe('returns', 'warehouse-orders', RecordingMessageHandler::class))
        ->toThrow(
            InvalidSubscriptionException::class,
            'Subscription [warehouse-orders] has already been registered.',
        );
    expect($subscriptions->find('warehouse-orders'))->toBe($registered);
});

test('rejects a queued-message drain name already used by an active subscription', function (): void {
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $replacement = $subscriptions->subscribe(
        'orders',
        'warehouse-order-processing-v2',
        RecordingMessageHandler::class,
    );

    expect(fn (): Subscription => $replacement->drainMessagesQueuedFor('warehouse-order-processing'))
        ->toThrow(InvalidSubscriptionException::class);
});

test('rejects an active subscription name already used for queued-message draining', function (): void {
    $subscriptions = new SubscriptionRegistry;
    $replacement = $subscriptions
        ->subscribe('orders', 'warehouse-order-processing-v2', RecordingMessageHandler::class)
        ->drainMessagesQueuedFor('warehouse-order-processing');

    expect(fn (): Subscription => $subscriptions->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class))
        ->toThrow(InvalidSubscriptionException::class);
    expect($subscriptions->resolveForQueuedMessage('warehouse-order-processing'))->toBe($replacement);
});

test('rejects a queued-message drain name already claimed by another subscription', function (): void {
    $subscriptions = new SubscriptionRegistry;
    $firstReplacement = $subscriptions
        ->subscribe('orders', 'warehouse-order-processing-v2', RecordingMessageHandler::class)
        ->drainMessagesQueuedFor('warehouse-order-processing');
    $secondReplacement = $subscriptions->subscribe(
        'orders',
        'warehouse-order-processing-v3',
        RecordingMessageHandler::class,
    );

    expect(fn (): Subscription => $secondReplacement->drainMessagesQueuedFor('warehouse-order-processing'))
        ->toThrow(InvalidSubscriptionException::class);
    expect($subscriptions->resolveForQueuedMessage('warehouse-order-processing'))->toBe($firstReplacement);
});

test('rejects handlers outside the message handler contract without reserving the subscription name', function (): void {
    $subscriptions = new SubscriptionRegistry;

    expect(fn (): Subscription => $subscriptions->subscribe('orders', 'warehouse-orders', stdClass::class))
        ->toThrow(InvalidSubscriptionException::class);
    $subscriptions->subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);
});

test('rejects the message handler interface without reserving the subscription name', function (): void {
    $subscriptions = new SubscriptionRegistry;
    $handler = MessageHandler::class;

    expect(fn (): Subscription => $subscriptions->subscribe('orders', 'warehouse-orders', $handler))
        ->toThrow(
            InvalidSubscriptionException::class,
            "Subscription handler [$handler] must be a concrete class implementing ".MessageHandler::class.'.',
        );
    $subscriptions->subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);
});

test('rejects non-portable subscription identifiers', function (string $topic, string $name, string $message): void {
    $subscriptions = new SubscriptionRegistry;

    expect(fn (): Subscription => $subscriptions->subscribe($topic, $name, RecordingMessageHandler::class))
        ->toThrow(InvalidSubscriptionException::class, $message);
})->with([
    'blank topic' => ['   ', 'warehouse-orders', 'Subscription topic [   ] must contain at least three ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.'],
    'blank name' => ['orders', '   ', 'Subscription name [   ] must contain at least three ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.'],
]);
