<?php

declare(strict_types=1);

use Illuminate\Contracts\Foundation\Application;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\SpoolrailServiceProvider;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

test('loads application subscription routes when booted without resolving a broker connection', function (): void {
    // --- Arrange ---
    app()->setBasePath(__DIR__.'/../Fixtures/application');

    config()->set('spoolrail.default', 'route-test');
    config()->set('spoolrail.connections.route-test', ['driver' => 'route-test']);

    $connectionsCreated = 0;
    Spoolrail::extend('route-test', function (Application $app, array $config, string $name) use (&$connectionsCreated): ArrayDriver {
        $connectionsCreated++;

        return new ArrayDriver(
            $name,
            'route-test',
            $app->make(SubscriptionRegistry::class),
        );
    });

    $provider = new SpoolrailServiceProvider(app());

    // --- Act ---
    $provider->boot();
    $subscription = app(SubscriptionRegistry::class)->get('route-loaded-orders');

    // --- Assert ---
    expect($subscription->topic())->toBe('orders');
    expect($connectionsCreated)->toBe(0);
});
