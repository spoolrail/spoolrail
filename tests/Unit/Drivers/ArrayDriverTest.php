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

test('restores the failed delivery and stops draining when its callback throws', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'retryable-orders', NoopMessageHandler::class);

    $driver = new ArrayDriver('array', 'array', $subscriptions);
    $driver->publish('orders', 'first order');
    $driver->publish('orders', 'second order');

    $bodies = [];
    $failure = new RuntimeException('Handoff failed.');

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('retryable-orders', function (string $body) use (&$bodies, $failure): void {
            $bodies[] = $body;

            throw $failure;
        });
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    $afterFailure = $bodies;

    $driver->consume('retryable-orders', function (string $body) use (&$bodies): void {
        $bodies[] = $body;
    });

    // --- Assert ---
    expect($caught)->toBe($failure);
    expect($afterFailure)->toBe(['first order']);
    expect($bodies)->toBe([
        'first order',
        'first order',
        'second order',
    ]);
});
