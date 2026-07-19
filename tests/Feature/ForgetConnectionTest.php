<?php

declare(strict_types=1);

use Illuminate\Contracts\Foundation\Application;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

test('rebuilds only the forgotten default connection on its next request', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'primary');
    config()->set('spoolrail.connections', [
        'primary' => ['driver' => 'testing'],
        'secondary' => ['driver' => 'testing'],
    ]);

    $created = [];
    Spoolrail::extend('testing', function (Application $app, array $_config, string $name) use (&$created): ArrayDriver {
        $created[] = $name;

        return new ArrayDriver(
            $name,
            'primary',
            $app->make(SubscriptionRegistry::class),
        );
    });

    $primary = Spoolrail::connection();
    $secondary = Spoolrail::connection('secondary');

    // --- Act ---
    Spoolrail::forgetConnection();
    $createdAfterForget = $created;
    $replacement = Spoolrail::connection();
    $remaining = Spoolrail::connection('secondary');

    // --- Assert ---
    expect($createdAfterForget)->toBe(['primary', 'secondary']);
    expect($replacement)->not->toBe($primary);
    expect($remaining)->toBe($secondary);
    expect($created)->toBe(['primary', 'secondary', 'primary']);
});
