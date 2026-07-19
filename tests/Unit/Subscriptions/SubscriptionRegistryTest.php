<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

test('selects subscriptions assigned to the requested broker connection', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;

    $primary = $subscriptions->subscribe('orders', 'primary-orders', NoopMessageHandler::class);
    $secondary = $subscriptions
        ->subscribe('orders', 'secondary-orders', NoopMessageHandler::class)
        ->onConnection('secondary');

    // --- Act ---
    $primarySubscriptions = $subscriptions->forTopic('orders', 'primary', 'primary');
    $secondarySubscriptions = $subscriptions->forTopic('orders', 'secondary', 'primary');

    // --- Assert ---
    expect($primarySubscriptions)->toBe([$primary]);
    expect($secondarySubscriptions)->toBe([$secondary]);
});

test('rejects a duplicate name without replacing the registered subscription', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $registered = $subscriptions->subscribe('orders', 'warehouse-orders', NoopMessageHandler::class);
    $replacementHandler = Mockery::mock(NoopMessageHandler::class);

    // --- Act ---
    $failure = null;

    try {
        $subscriptions->subscribe('returns', 'warehouse-orders', $replacementHandler::class);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $current = $subscriptions->get('warehouse-orders');

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidSubscriptionException::class);
    expect($failure?->getMessage())->toBe('Subscription [warehouse-orders] has already been registered.');
    expect($current)->toBe($registered);
});

test('rejects handlers outside the message handler contract without reserving the subscription name', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;

    // --- Act ---
    $failure = null;

    try {
        $subscriptions->subscribe('orders', 'warehouse-orders', stdClass::class);
    } catch (InvalidSubscriptionException $exception) {
        $failure = $exception;
    }

    $subscriptions->subscribe('orders', 'warehouse-orders', NoopMessageHandler::class);

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidSubscriptionException::class);
});

test('rejects blank subscription identifiers', function (string $topic, string $name, string $message): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;

    // --- Act ---
    $failure = null;

    try {
        $subscriptions->subscribe($topic, $name, NoopMessageHandler::class);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidSubscriptionException::class);
    expect($failure?->getMessage())->toBe($message);
})->with([
    'topic' => ['   ', 'warehouse-orders', 'Subscription topic must not be empty.'],
    'name' => ['orders', '   ', 'Subscription name must not be empty.'],
]);
