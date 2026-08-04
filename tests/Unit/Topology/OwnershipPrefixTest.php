<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

test('uses a valid configured ownership prefix without normalizing it', function (): void {
    $prefix = 'W'.str_repeat('a', 23);
    config()->set('spoolrail.prefix', $prefix);

    expect(app(OwnershipPrefix::class)->current())
        ->toBe($prefix);
});

test('rejects an invalid configured ownership prefix instead of normalizing it', function (): void {
    config()->set('spoolrail.prefix', 'Warehouse Production');

    expect(fn () => app(OwnershipPrefix::class)->current())
        ->toThrow(InvalidConfigException::class);
});

test('requires an explicit ownership prefix for receive-side operations', function (): void {
    config()->set('spoolrail.prefix');

    expect(fn () => app(OwnershipPrefix::class)->current())
        ->toThrow(
            InvalidConfigException::class,
            'Spoolrail ownership prefix is required for receive-side operations. Set [SPOOLRAIL_PREFIX] to a stable application identifier.',
        );
});

test('rejects an ownership prefix beyond the portable budget', function (): void {
    config()->set('spoolrail.prefix', 'p'.str_repeat('r', 24));

    expect(fn () => app(OwnershipPrefix::class)->current())
        ->toThrow(InvalidConfigException::class);
});

test('rejects a transport-reserved ownership prefix case-insensitively', function (): void {
    config()->set('spoolrail.prefix', 'GoOg-warehouse');

    expect(fn () => app(OwnershipPrefix::class)->current())
        ->toThrow(InvalidConfigException::class);
});
