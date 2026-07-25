<?php

declare(strict_types=1);

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
    $driver->consume('competing-orders', function (string $first) use ($driver, &$bodies): void {
        $bodies[] = $first;

        $driver->consume('competing-orders', function (string $second) use (&$bodies): void {
            $bodies[] = $second;
        });
    });

    // --- Assert ---
    expect($bodies)->toBe(['first order', 'second order']);
});
