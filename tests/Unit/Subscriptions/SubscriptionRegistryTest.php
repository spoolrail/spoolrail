<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\AbstractMessageHandler;
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

test('rejects a queued-message drain name already used by an active subscription', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'warehouse-order-processing', NoopMessageHandler::class);
    $replacement = $subscriptions->subscribe(
        'orders',
        'warehouse-order-processing-v2',
        NoopMessageHandler::class,
    );

    // --- Act ---
    $failure = null;

    try {
        $replacement->drainMessagesQueuedFor('warehouse-order-processing');
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidSubscriptionException::class);
});

test('rejects an active subscription name already used for queued-message draining', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $replacement = $subscriptions
        ->subscribe('orders', 'warehouse-order-processing-v2', NoopMessageHandler::class)
        ->drainMessagesQueuedFor('warehouse-order-processing');

    // --- Act ---
    $failure = null;

    try {
        $subscriptions->subscribe('orders', 'warehouse-order-processing', NoopMessageHandler::class);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidSubscriptionException::class);
    expect($subscriptions->getForQueuedMessage('warehouse-order-processing'))->toBe($replacement);
});

test('rejects a queued-message drain name already claimed by another subscription', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $firstReplacement = $subscriptions
        ->subscribe('orders', 'warehouse-order-processing-v2', NoopMessageHandler::class)
        ->drainMessagesQueuedFor('warehouse-order-processing');
    $secondReplacement = $subscriptions->subscribe(
        'orders',
        'warehouse-order-processing-v3',
        NoopMessageHandler::class,
    );

    // --- Act ---
    $failure = null;

    try {
        $secondReplacement->drainMessagesQueuedFor('warehouse-order-processing');
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidSubscriptionException::class);
    expect($subscriptions->getForQueuedMessage('warehouse-order-processing'))->toBe($firstReplacement);
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

test('rejects non-concrete message handlers without reserving the subscription name', function (string $handler): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;

    // --- Act ---
    $failure = null;

    try {
        $subscriptions->subscribe('orders', 'warehouse-orders', $handler);
    } catch (InvalidSubscriptionException $exception) {
        $failure = $exception;
    }

    $subscriptions->subscribe('orders', 'warehouse-orders', NoopMessageHandler::class);

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidSubscriptionException::class);
    expect($failure?->getMessage())->toBe(
        "Subscription handler [$handler] must be a concrete class implementing ".MessageHandler::class.'.',
    );
})->with([
    'interface' => MessageHandler::class,
    'abstract class' => AbstractMessageHandler::class,
]);

test('rejects non-portable subscription identifiers', function (string $topic, string $name, string $message): void {
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
    'blank topic' => ['   ', 'warehouse-orders', 'Subscription topic [   ] must contain at least three ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.'],
    'blank name' => ['orders', '   ', 'Subscription name [   ] must contain at least three ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.'],
]);
