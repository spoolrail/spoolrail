<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

test('rejects blank subscription settings', function (Closure $configure, string $message): void {
    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-orders', NoopMessageHandler::class);

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
        'Subscription name [   ] must contain at least three ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.',
    ],
]);
