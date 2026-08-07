<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\SpoolrailServiceProvider;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

test('publishes the package config under the spoolrail config tag', function (): void {
    // --- Act ---
    $paths = SpoolrailServiceProvider::pathsToPublish(
        SpoolrailServiceProvider::class,
        'spoolrail-config',
    );

    // --- Assert ---
    expect($paths)->toHaveCount(1);

    $source = array_key_first($paths);

    expect(realpath($source))->toBe(realpath(dirname(__DIR__, 2).'/config/spoolrail.php'));
    expect($paths[$source])->toBe(config_path('spoolrail.php'));
});

test('publishes the outbox migration under the spoolrail migrations tag', function (): void {
    // --- Act ---
    $paths = SpoolrailServiceProvider::pathsToPublish(
        SpoolrailServiceProvider::class,
        'spoolrail-migrations',
    );

    // --- Assert ---
    expect($paths)->toHaveCount(1);

    $source = array_key_first($paths);

    expect(realpath($source))->toBe(realpath(
        dirname(__DIR__, 2).'/database/migrations/0001_01_01_000000_create_outbox_publications_table.php',
    ));
    expect($paths[$source])->toBe(
        database_path('migrations/0001_01_01_000000_create_outbox_publications_table.php'),
    );
});

test('loads the direct publication policy and outbox defaults', function (): void {
    expect(config('spoolrail.outbox'))->toBe([
        'enabled' => false,
        'connection' => null,
        'exception_cooldown' => 300,
    ]);
});

test('loads the Queue handoff idempotency defaults', function (): void {
    expect(config('spoolrail.handoff_idempotency'))->toBe([
        'cache_store' => 'array',
        'expiry' => 600,
    ]);
});

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
    expect($connection['prefetch'])->toBe(10);
    expect($connection['management']['url'])->toBe('http://127.0.0.1:15672');
    expect($connection['management']['username'])->toBe('spoolrail');
    expect($connection['management']['password'])->toBe('spoolrail');
    expect($connection['management']['ca_file'])->toBeNull();
});

test('loads application subscription routes without deriving an ownership prefix or resolving a broker connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.prefix');

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
    $subscription = app(SubscriptionRegistry::class)->findOrFail('route-loaded-orders');

    // --- Assert ---
    expect(config('spoolrail.prefix'))->toBeNull();
    expect($subscription->topic())->toBe('orders');
});
