<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Http;
use Spoolrail\Spoolrail\Exceptions\InvalidPhysicalNameException;
use Spoolrail\Spoolrail\Exceptions\InvalidRabbitMqTopicNameException;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

test('rejects an over-limit consumer queue name before opening an AMQP connection', function (): void {
    // --- Arrange ---
    $subscription = 'orders';

    config()->set('spoolrail.default', 'rabbit');
    config()->set('spoolrail.prefix', 'a'.str_repeat('b', 249));
    config()->set('spoolrail.connections.rabbit', [
        'driver' => 'rabbitmq',
        'url' => 'amqp://guest:guest@rabbit.invalid/%2F',
    ]);

    Spoolrail::subscribe('orders', $subscription, NoopMessageHandler::class);

    // --- Act ---
    $failure = null;

    try {
        $this->artisan("spoolrail:consume $subscription")->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidPhysicalNameException::class);
});

test('rejects an over-limit published topic before opening an AMQP connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'rabbit');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections.rabbit', [
        'driver' => 'rabbitmq',
        'url' => 'amqp://guest:guest@rabbit.invalid/%2F',
    ]);

    // --- Act ---
    $failure = null;

    try {
        Spoolrail::publish(
            str_repeat('a', 256),
            Message::make('order.created', []),
        );
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidRabbitMqTopicNameException::class);
});

test('rejects over-limit declared topology names before Management HTTP discovery', function (string $topic, string $prefix, string $exception): void {
    // --- Arrange ---
    Http::preventStrayRequests();

    config()->set('spoolrail.default', 'rabbit');
    config()->set('spoolrail.prefix', $prefix);
    config()->set('spoolrail.connections.rabbit', [
        'driver' => 'rabbitmq',
        'url' => 'amqp://guest:guest@rabbit.internal/%2F',
    ]);

    Spoolrail::subscribe($topic, 'orders', NoopMessageHandler::class);

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:sync')->run();
    } catch (Throwable $caught) {
        $failure = $caught;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(TopologyPreflightException::class);
    expect($failure?->failures['rabbit'] ?? null)->toBeInstanceOf($exception);
    Http::assertNothingSent();
})->with([
    'topic' => [
        str_repeat('a', 256),
        'warehouse-production',
        InvalidRabbitMqTopicNameException::class,
    ],
    'queue with ownership prefix' => [
        'orders',
        'a'.str_repeat('b', 249),
        InvalidPhysicalNameException::class,
    ],
]);

test('rejects over-limit undeclared-topology names before Management HTTP discovery', function (): void {
    // --- Arrange ---
    Http::preventStrayRequests();

    config()->set('spoolrail.default', 'rabbit');
    config()->set('spoolrail.prefix', 'a'.str_repeat('b', 249));
    config()->set('spoolrail.connections.rabbit', [
        'driver' => 'rabbitmq',
        'url' => 'amqp://guest:guest@rabbit.internal/%2F',
    ]);

    Spoolrail::subscribe('orders', 'orders', NoopMessageHandler::class);

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:delete-undeclared-subscriptions')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidPhysicalNameException::class);
    Http::assertNothingSent();
});

test('rejects an over-limit manually deleted topic before Management HTTP discovery', function (): void {
    // --- Arrange ---
    Http::preventStrayRequests();

    config()->set('spoolrail.default', 'rabbit');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections.rabbit', [
        'driver' => 'rabbitmq',
        'url' => 'amqp://guest:guest@rabbit.internal/%2F',
    ]);

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:delete-topic '.str_repeat('a', 256))->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidRabbitMqTopicNameException::class);
    Http::assertNothingSent();
});
