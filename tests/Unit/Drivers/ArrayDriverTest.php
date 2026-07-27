<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

test('reserves an in-flight delivery from a competing consumer', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'competing-orders', RecordingMessageHandler::class);

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

test('releases a failed handoff and stops the current delivery drain', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'failing-orders', RecordingMessageHandler::class);

    $driver = new ArrayDriver('array', 'array', $subscriptions);
    $driver->publish('orders', 'first order');
    $driver->publish('orders', 'second order');

    $handoffs = [];
    $failure = new RuntimeException('Queue handoff failed.');

    // --- Act ---
    try {
        $driver->consume('failing-orders', function (string $body) use (&$handoffs, $failure): void {
            $handoffs[] = $body;

            throw $failure;
        });
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    $remaining = [];
    $driver->consume('failing-orders', function (string $body) use (&$remaining): void {
        $remaining[] = $body;
    });

    // --- Assert ---
    expect($caught ?? null)->toBe($failure);
    expect($handoffs)->toBe(['first order']);
    expect($remaining)->toBe(['first order', 'second order']);
});
