<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\SpoolrailServiceProvider;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

test('loads the built-in RabbitMQ connection template', function (): void {
    $connection = config('spoolrail.connections.rabbitmq');

    expect($connection)->toBeArray();
    expect($connection['driver'])->toBe('rabbitmq');
    expect($connection['scheme'])->toBe('amqp');
    expect($connection['host'])->toBe('127.0.0.1');
    expect($connection['username'])->toBe('spoolrail');
    expect($connection['password'])->toBe('spoolrail');
    expect($connection['ca_file'])->toBeNull();
    expect($connection['connection_timeout'])->toBe(3);
    expect($connection['publisher_confirm_timeout'])->toBe(60);
    expect($connection['consumer_ack_timeout'])->toBeNull();
    expect($connection['prefetch'])->toBe(10);
    expect($connection['management']['url'])->toBe('http://127.0.0.1:15672');
    expect($connection['management']['username'])->toBe('spoolrail');
    expect($connection['management']['password'])->toBe('spoolrail');
    expect($connection['management']['ca_file'])->toBeNull();
});

test('loads application subscription routes when booted without resolving a broker connection', function (): void {
    // --- Arrange ---
    $bootstrapPath = app()->bootstrapPath();
    app()
        ->setBasePath(__DIR__.'/../Fixtures/application')
        ->useBootstrapPath($bootstrapPath);

    Spoolrail::extend(
        'array',
        static fn (): never => throw new RuntimeException('Subscription route loading resolved a broker connection.'),
    );

    $provider = new SpoolrailServiceProvider(app());

    // --- Act ---
    $provider->boot();
    $subscription = app(SubscriptionRegistry::class)->get('route-loaded-orders');

    // --- Assert ---
    expect($subscription->topic())->toBe('orders');
});
