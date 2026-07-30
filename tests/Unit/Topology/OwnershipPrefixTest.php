<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

test('uses a valid configured ownership prefix without normalizing it', function (): void {
    config()->set('spoolrail.prefix', 'Warehouse_Production');

    expect(app(OwnershipPrefix::class)->current())
        ->toBe('Warehouse_Production');
});

test('rejects an invalid configured ownership prefix instead of normalizing it', function (): void {
    config()->set('spoolrail.prefix', 'Warehouse Production');

    expect(fn () => app(OwnershipPrefix::class)->current())
        ->toThrow(InvalidConfigException::class);
});
