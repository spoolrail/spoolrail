<?php

declare(strict_types=1);

use Illuminate\Contracts\Foundation\Application;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigurationException;
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

test('rejects an undefined connection', function (): void {
    expect(fn () => Spoolrail::connection('missing'))
        ->toThrow(InvalidConfigurationException::class, 'Spoolrail connection [missing] is not defined.');
});

test('rejects a connection without a declared driver', function (): void {
    config()->set('spoolrail.connections.invalid', [
        'label' => 'missing-driver',
    ]);

    expect(fn () => Spoolrail::connection('invalid'))
        ->toThrow(
            InvalidConfigurationException::class,
            'Spoolrail connection [invalid] must define a non-empty string [driver].',
        );
});

test('rejects an unsupported driver', function (): void {
    config()->set('spoolrail.connections.invalid', [
        'driver' => 'missing',
    ]);

    expect(fn () => Spoolrail::connection('invalid'))
        ->toThrow(InvalidConfigurationException::class, 'Spoolrail driver [missing] is not supported.');
});
