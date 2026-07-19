<?php

declare(strict_types=1);

use Illuminate\Contracts\Foundation\Application;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigurationException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

test('creates each configured connection only when requested and caches it by name', function (): void {
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

    $createdBeforeRequest = $created;

    // --- Act ---
    $default = Spoolrail::connection();
    $secondary = Spoolrail::connection('secondary');
    $cachedDefault = Spoolrail::connection('primary');
    $cachedSecondary = Spoolrail::connection('secondary');

    // --- Assert ---
    expect($createdBeforeRequest)->toBe([]);
    expect($default)->toBe($cachedDefault);
    expect($secondary)->toBe($cachedSecondary);
    expect($default)->not->toBe($secondary);
    expect($created)->toBe(['primary', 'secondary']);
});

test('passes connection identity and unchanged configuration to a custom driver factory', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.custom', [
        'driver' => 'custom',
        'label' => 'registered',
        'positional-value',
    ]);

    $receivedApplication = null;
    $receivedConfiguration = null;
    $receivedName = null;

    Spoolrail::extend('custom', function (Application $app, array $config, string $name) use (&$receivedApplication, &$receivedConfiguration, &$receivedName): ArrayDriver {
        $receivedApplication = $app;
        $receivedConfiguration = $config;
        $receivedName = $name;

        return new ArrayDriver(
            $name,
            $name,
            $app->make(SubscriptionRegistry::class),
        );
    });

    // --- Act ---
    Spoolrail::connection('custom');

    // --- Assert ---
    expect($receivedApplication)->toBe(app());
    expect($receivedName)->toBe('custom');
    expect($receivedConfiguration)->toBe([
        'driver' => 'custom',
        'label' => 'registered',
        'positional-value',
    ]);
});

test('rejects an undefined connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.missing', null);

    // --- Act ---
    $failure = null;

    try {
        Spoolrail::connection('missing');
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidConfigurationException::class);
    expect($failure?->getMessage())->toBe('Spoolrail connection [missing] is not defined.');
});

test('rejects a connection without a declared driver', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.invalid', [
        'label' => 'missing-driver',
    ]);

    // --- Act ---
    $failure = null;

    try {
        Spoolrail::connection('invalid');
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidConfigurationException::class);
    expect($failure?->getMessage())->toBe('Spoolrail connection [invalid] must define a non-empty string [driver].');
});

test('rejects an unsupported driver', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.invalid', [
        'driver' => 'missing',
    ]);

    // --- Act ---
    $failure = null;

    try {
        Spoolrail::connection('invalid');
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidConfigurationException::class);
    expect($failure?->getMessage())->toBe('Spoolrail driver [missing] is not supported.');
});
