<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidPhysicalNameException;
use Spoolrail\Spoolrail\Exceptions\InvalidRabbitMqTopicNameException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqName;

test('derives a RabbitMQ queue from the ownership prefix and logical subscription', function (): void {
    expect(RabbitMqName::queue('warehouse-production', 'order-imports'))
        ->toBe('warehouse-production-order-imports');
});

test('accepts a complete queue name at the transport limit and rejects the next byte', function (): void {
    // --- Arrange ---
    $atLimit = 'a'.str_repeat('b', 250);
    $overLimit = "{$atLimit}c";

    // --- Act ---
    $physicalName = RabbitMqName::queue('app', $atLimit);

    $failure = null;

    try {
        RabbitMqName::queue('app', $overLimit);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect(strlen($physicalName))->toBe(RabbitMqName::MAX_BYTES);
    expect($failure)->toBeInstanceOf(InvalidPhysicalNameException::class);
    expect($failure?->getMessage())->toContain("Logical subscription [$overLimit]");
    expect($failure?->getMessage())->toContain("RabbitMQ queue [app-$overLimit]");
    expect($failure?->getMessage())->toContain('ownership prefix [app]');
    expect($failure?->getMessage())->toContain('255-byte transport limit');
});

test('accepts a topic at the transport limit and rejects the next byte', function (): void {
    // --- Arrange ---
    $atLimit = 'a'.str_repeat('b', 254);
    $overLimit = "{$atLimit}c";

    // --- Act ---
    $topic = RabbitMqName::topic($atLimit);

    $failure = null;

    try {
        RabbitMqName::topic($overLimit);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($topic)->toBe($atLimit);
    expect($failure)->toBeInstanceOf(InvalidRabbitMqTopicNameException::class);
    expect($failure?->getMessage())->toContain("Logical topic [$overLimit]");
    expect($failure?->getMessage())->toContain('255-byte transport limit');
});
