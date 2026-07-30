<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;

test('rebuilds only the forgotten default connection on its next request', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.secondary', ['driver' => 'array']);

    $defaultConnection = Spoolrail::connection();
    $secondaryConnection = Spoolrail::connection('secondary');

    // --- Act ---
    Spoolrail::forgetConnection();
    $replacementConnection = Spoolrail::connection();
    $remainingConnection = Spoolrail::connection('secondary');

    // --- Assert ---
    expect($replacementConnection)->not->toBe($defaultConnection);
    expect($remainingConnection)->toBe($secondaryConnection);
});
