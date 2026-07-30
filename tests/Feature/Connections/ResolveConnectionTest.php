<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Facades\Spoolrail;

test('resolves and caches each configured ArrayDriver connection', function (): void {
    config()->set('spoolrail.connections.secondary', ['driver' => 'array']);

    $defaultConnection = Spoolrail::connection();
    $secondaryConnection = Spoolrail::connection('secondary');

    expect(Spoolrail::connection())->toBe($defaultConnection);
    expect(Spoolrail::connection('secondary'))->toBe($secondaryConnection);
    expect($secondaryConnection)->not->toBe($defaultConnection);
});

test('rejects an undefined connection', function (): void {
    expect(fn () => Spoolrail::connection('missing'))
        ->toThrow(InvalidConfigException::class, 'Spoolrail connection [missing] is not defined.');
});

test('rejects an invalid default connection', function (): void {
    config()->set('spoolrail.default', ' ');

    expect(fn () => Spoolrail::connection())
        ->toThrow(
            InvalidConfigException::class,
            'Spoolrail default connection must be a non-empty string.',
        );
});

test('rejects a connection whose config is not an array', function (): void {
    config()->set('spoolrail.connections.invalid', 'array');

    expect(fn () => Spoolrail::connection('invalid'))
        ->toThrow(
            InvalidConfigException::class,
            'Spoolrail connection [invalid] config must be an array.',
        );
});

test('rejects a connection without a declared driver', function (): void {
    config()->set('spoolrail.connections.invalid', [
        'label' => 'missing-driver',
    ]);

    expect(fn () => Spoolrail::connection('invalid'))
        ->toThrow(
            InvalidConfigException::class,
            'Spoolrail connection [invalid] must define a non-empty string [driver].',
        );
});

test('rejects an unsupported driver', function (): void {
    config()->set('spoolrail.connections.invalid', [
        'driver' => 'missing',
    ]);

    expect(fn () => Spoolrail::connection('invalid'))
        ->toThrow(InvalidConfigException::class, 'Spoolrail driver [missing] is not supported.');
});
