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
    $atLimit = 'a'.str_repeat('b', 250);
    $overLimit = "{$atLimit}c";

    $physicalName = RabbitMqName::queue('app', $atLimit);

    expect(strlen($physicalName))->toBe(RabbitMqName::MAX_BYTES);
    expect(fn (): string => RabbitMqName::queue('app', $overLimit))
        ->toThrow(function (InvalidPhysicalNameException $exception) use ($overLimit): void {
            expect($exception->getMessage())->toContain("Logical subscription [$overLimit]");
            expect($exception->getMessage())->toContain("RabbitMQ queue [app-$overLimit]");
            expect($exception->getMessage())->toContain('ownership prefix [app]');
            expect($exception->getMessage())->toContain('255-byte transport limit');
        });
});

test('accepts a topic at the transport limit and rejects the next byte', function (): void {
    $atLimit = 'a'.str_repeat('b', 254);
    $overLimit = "{$atLimit}c";

    $topic = RabbitMqName::topic($atLimit);

    expect($topic)->toBe($atLimit);
    expect(fn (): string => RabbitMqName::topic($overLimit))
        ->toThrow(function (InvalidRabbitMqTopicNameException $exception) use ($overLimit): void {
            expect($exception->getMessage())->toContain("Logical topic [$overLimit]");
            expect($exception->getMessage())->toContain('255-byte transport limit');
        });
});
