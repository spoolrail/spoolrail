<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

test('rejects blank subscription settings', function (Closure $configure, string $message): void {
    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);

    expect(fn () => $configure($subscription))
        ->toThrow(InvalidSubscriptionException::class, $message);
})->with([
    'broker connection' => [
        fn (Subscription $subscription): Subscription => $subscription->onConnection('   '),
        'Spoolrail connection must not be empty.',
    ],
    'queue connection' => [
        fn (Subscription $subscription): Subscription => $subscription->onQueueConnection('   '),
        'Queue connection must not be empty.',
    ],
    'queue' => [
        fn (Subscription $subscription): Subscription => $subscription->onQueue('   '),
        'Queue must not be empty.',
    ],
    'queued subscription' => [
        fn (Subscription $subscription): Subscription => $subscription->drainMessagesQueuedFor('   '),
        'Subscription name [   ] must contain between 3 and 50 ASCII characters',
    ],
]);

test('rejects a queued-message drain name beyond the portable subscription limit', function (): void {
    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-orders-v2', RecordingMessageHandler::class);

    expect(fn (): Subscription => $subscription->drainMessagesQueuedFor('s'.str_repeat('u', 50)))
        ->toThrow(InvalidSubscriptionException::class);
});
