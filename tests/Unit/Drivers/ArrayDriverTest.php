<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

test('reserves one delivery per receive attempt', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'competing-orders', RecordingMessageHandler::class);
    $driver = new ArrayDriver('array', 'array', $subscriptions);
    $driver->publish('orders', 'first order', []);
    $driver->publish('orders', 'second order', []);
    $received = [];

    // --- Act ---
    $driver->receive('competing-orders', function (array $deliveries) use (&$received): void {
        $received[] = $deliveries[0];
    }, static function (): void {});
    $driver->receive('competing-orders', function (array $deliveries) use (&$received): void {
        $received[] = $deliveries[0];
    }, static function (): void {});

    // --- Assert ---
    expect(array_map(
        static fn (Delivery $delivery): string => $delivery->body,
        $received,
    ))->toBe(['first order', 'second order']);
});

test('releases a delivery for prompt redelivery', function (): void {
    // --- Arrange ---
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'failing-orders', RecordingMessageHandler::class);
    $driver = new ArrayDriver('array', 'array', $subscriptions);
    $driver->publish('orders', 'first order', ['correlation-id' => 'first']);
    $delivery = null;
    $driver->receive('failing-orders', function (array $deliveries) use (&$delivery): void {
        $delivery = $deliveries[0];
    }, static function (): void {});

    // --- Act ---
    $released = false;
    $driver->release(
        $delivery,
        function () use (&$released): void {
            $released = true;
        },
        static function (): void {},
    );
    $redelivery = null;
    $driver->receive('failing-orders', function (array $deliveries) use (&$redelivery): void {
        $redelivery = $deliveries[0];
    }, static function (): void {});

    // --- Assert ---
    expect($released)->toBeTrue();
    expect($redelivery->body)->toBe('first order');
    expect($redelivery->headers)->toBe(['correlation-id' => 'first']);
    expect($redelivery->redelivered)->toBeTrue();
});
