<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\TransportContext;

test('reserves an in-flight delivery from a competing consumer', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'competing-orders', RecordingMessageHandler::class);

    $driver = new ArrayDriver('array', 'array', $subscriptions);
    $driver->publish('orders', 'first order', ['correlation-id' => 'first']);
    $driver->publish('orders', 'second order', ['correlation-id' => 'second']);

    $bodies = [];

    // --- Act ---
    $driver->consume('competing-orders', function (string $first, TransportContext $_transport) use ($driver, &$bodies): void {
        $bodies[] = $first;

        $driver->consume('competing-orders', function (string $second, TransportContext $_transport) use (&$bodies): void {
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
    $driver->publish('orders', 'first order', ['correlation-id' => 'first']);
    $driver->publish('orders', 'second order', ['correlation-id' => 'second']);

    $handoffs = [];
    $contexts = [];
    $failure = new RuntimeException('Queue handoff failed.');

    // --- Act ---
    try {
        $driver->consume('failing-orders', function (string $body, TransportContext $transport) use (&$handoffs, &$contexts, $failure): void {
            $handoffs[] = $body;
            $contexts[] = $transport;

            throw $failure;
        });
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    $remaining = [];
    $driver->consume('failing-orders', function (string $body, TransportContext $transport) use (&$remaining, &$contexts): void {
        $remaining[] = $body;
        $contexts[] = $transport;
    });

    // --- Assert ---
    expect($caught ?? null)->toBe($failure);
    expect($handoffs)->toBe(['first order']);
    expect($remaining)->toBe(['first order', 'second order']);
    expect($contexts[0])->not->toBe($contexts[1]);
    expect($contexts[0]->driver)->toBe('array');
    expect($contexts[0]->connectionName)->toBe('array');
    expect($contexts[0]->topic)->toBe('orders');
    expect($contexts[0]->subscription)->toBe('failing-orders');
    expect($contexts[0]->headers)->toBe(['correlation-id' => 'first']);
    expect($contexts[0]->transportMessageId)->toBeNull();
    expect($contexts[0]->transportPublishedAt)->toBeNull();
    expect($contexts[0]->redelivered)->toBeFalse();
    expect($contexts[1]->headers)->toBe(['correlation-id' => 'first']);
    expect($contexts[1]->redelivered)->toBeTrue();
    expect($contexts[2]->headers)->toBe(['correlation-id' => 'second']);
    expect($contexts[2]->redelivered)->toBeFalse();
});
