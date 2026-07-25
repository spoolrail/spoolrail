<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;

test('rebuilds only the forgotten default connection on its next request', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.secondary', ['driver' => 'array']);

    $default = Spoolrail::connection();
    $secondary = Spoolrail::connection('secondary');

    // --- Act ---
    Spoolrail::forgetConnection();
    $replacement = Spoolrail::connection();
    $remaining = Spoolrail::connection('secondary');

    // --- Assert ---
    expect($replacement)->not->toBe($default);
    expect($remaining)->toBe($secondary);
});
