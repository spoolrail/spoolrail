<?php

declare(strict_types=1);

use Illuminate\Contracts\Console\Kernel;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\SpoolrailServiceProvider;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

test('registers the public commands and hidden consumer runtime', function (): void {
    // --- Act ---
    $commands = app(Kernel::class)->all();

    // --- Assert ---
    expect($commands)->toHaveKeys(['spoolrail', 'spoolrail:terminate', 'spoolrail:consume']);
    expect($commands['spoolrail']->isHidden())->toBeFalse();
    expect($commands['spoolrail:consume']->isHidden())->toBeTrue();
});

test('publishes the package config under the spoolrail config tag', function (): void {
    // --- Act ---
    $paths = SpoolrailServiceProvider::pathsToPublish(
        SpoolrailServiceProvider::class,
        'spoolrail-config',
    );

    // --- Assert ---
    $source = dirname(__DIR__, 2).'/config/spoolrail.php';
    $destination = collect($paths)->first(
        fn (string $_destination, string $publishedSource): bool => realpath($publishedSource) === realpath($source),
    );

    expect($destination)->toBe(config_path('spoolrail.php'));
});

test('publishes the outbox migration under the spoolrail migrations tag', function (): void {
    // --- Act ---
    $paths = SpoolrailServiceProvider::pathsToPublish(
        SpoolrailServiceProvider::class,
        'spoolrail-migrations',
    );

    // --- Assert ---
    $source = dirname(__DIR__, 2).'/database/migrations/0001_01_01_000000_create_outbox_publications_table.php';
    $destination = collect($paths)->first(
        fn (string $_destination, string $publishedSource): bool => realpath($publishedSource) === realpath($source),
    );

    expect($destination)->toBe(
        database_path('migrations/0001_01_01_000000_create_outbox_publications_table.php'),
    );
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
