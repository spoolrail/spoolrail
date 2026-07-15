<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\SpoolrailServiceProvider;

test('registers the service provider', function (): void {
    expect(app()->getProviders(SpoolrailServiceProvider::class))
        ->not->toBeEmpty();
});
