<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidConfigurationException;
use Spoolrail\Spoolrail\OwnershipPrefix;

test('uses a valid configured ownership prefix without normalizing it', function (): void {
    config()->set('spoolrail.prefix', 'Warehouse_Production');

    expect(app(OwnershipPrefix::class)->value())
        ->toBe('Warehouse_Production');
});

test('rejects an invalid configured ownership prefix instead of normalizing it', function (): void {
    config()->set('spoolrail.prefix', 'Warehouse Production');

    expect(fn () => app(OwnershipPrefix::class)->value())
        ->toThrow(InvalidConfigurationException::class);
});
