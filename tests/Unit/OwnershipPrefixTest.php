<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidConfigurationException;
use Spoolrail\Spoolrail\OwnershipPrefix;

test('derives the ownership prefix from hyphenated application and environment slugs', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.prefix', null);
    config()->set('app.name', 'Warehouse API');
    config()->set('app.env', 'Production East');

    // --- Act ---
    $prefix = app(OwnershipPrefix::class)->value();

    // --- Assert ---
    expect($prefix)->toBe('warehouse-api-production-east');
});

test('uses a valid configured ownership prefix without normalizing it', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.prefix', 'Warehouse_Production');

    // --- Act ---
    $prefix = app(OwnershipPrefix::class)->value();

    // --- Assert ---
    expect($prefix)->toBe('Warehouse_Production');
});

test('rejects an invalid configured ownership prefix instead of normalizing it', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.prefix', 'Warehouse Production');

    // --- Act ---
    $failure = null;

    try {
        app(OwnershipPrefix::class)->value();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidConfigurationException::class);
});
