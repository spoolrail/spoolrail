<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Contracts\Delivery;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

test('reserves an in-flight delivery from a competing consumer', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'competing-orders', NoopMessageHandler::class);

    $driver = new ArrayDriver('array', 'array', $subscriptions);
    $driver->publish('orders', 'first order');
    $driver->publish('orders', 'second order');

    $bodies = [];

    // --- Act ---
    $driver->consume('competing-orders', function (Delivery $first) use ($driver, &$bodies): void {
        $bodies[] = $first->body();

        $driver->consume('competing-orders', function (Delivery $second) use (&$bodies): void {
            $bodies[] = $second->body();
            $second->acknowledge();
        });

        $first->acknowledge();
    });

    // --- Assert ---
    expect($bodies)->toBe(['first order', 'second order']);
});

test('redelivers a delivery when its callback returns without acknowledging', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'retryable-orders', NoopMessageHandler::class);

    $driver = new ArrayDriver('array', 'array', $subscriptions);
    $driver->publish('orders', 'order awaiting acknowledgement');

    $bodies = [];

    // --- Act ---
    $driver->consume('retryable-orders', function (Delivery $delivery) use (&$bodies): void {
        $bodies[] = $delivery->body();
    });

    $driver->consume('retryable-orders', function (Delivery $delivery) use (&$bodies): void {
        $bodies[] = $delivery->body();
        $delivery->acknowledge();
    });

    // --- Assert ---
    expect($bodies)->toBe([
        'order awaiting acknowledgement',
        'order awaiting acknowledgement',
    ]);
});
