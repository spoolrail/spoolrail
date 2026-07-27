<?php

declare(strict_types=1);

use Illuminate\Contracts\Foundation\Application;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Facades\Spoolrail;

test('lazily creates and caches a custom connection with its unchanged configuration', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.custom', [
        'driver' => 'custom',
        'label' => 'registered',
    ]);

    $driver = Mockery::mock(Driver::class);
    $created = 0;
    $receivedApplication = null;
    $receivedConfiguration = null;
    $receivedName = null;

    Spoolrail::extend('custom', function (Application $app, array $config, string $name) use ($driver, &$created, &$receivedApplication, &$receivedConfiguration, &$receivedName): Driver {
        $created++;
        $receivedApplication = $app;
        $receivedConfiguration = $config;
        $receivedName = $name;

        return $driver;
    });

    // --- Act ---
    $createdBeforeRequest = $created;
    $connection = Spoolrail::connection('custom');
    $cached = Spoolrail::connection('custom');

    // --- Assert ---
    expect($createdBeforeRequest)->toBe(0);
    expect($created)->toBe(1);
    expect($connection)->toBe($cached);
    expect($receivedApplication)->toBe(app());
    expect($receivedName)->toBe('custom');
    expect($receivedConfiguration)->toBe([
        'driver' => 'custom',
        'label' => 'registered',
    ]);
});
