<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\RabbitMq\ResourceName;

test('derives a queue name from the ownership prefix and logical subscription', function (): void {
    expect(ResourceName::queue('warehouse-production', 'order-imports'))
        ->toBe('warehouse-production-order-imports');
});

test('accepts a complete queue name at the transport limit and rejects the next byte', function (): void {
    $atLimit = 'a'.str_repeat('b', 250);
    $overLimit = "{$atLimit}c";

    $queueName = ResourceName::queue('app', $atLimit);

    expect(strlen($queueName))->toBe(ResourceName::MAX_BYTES);
    expect(fn (): string => ResourceName::queue('app', $overLimit))
        ->toThrow(function (LengthException $exception) use ($overLimit): void {
            expect($exception->getMessage())
                ->toContain("Logical subscription [$overLimit]")
                ->toContain("RabbitMQ queue [app-$overLimit]")
                ->toContain('ownership prefix [app]')
                ->toContain('255-byte transport limit');
        });
});

test('accepts a topic at the transport limit and rejects the next byte', function (): void {
    $atLimit = 'a'.str_repeat('b', 254);
    $overLimit = "{$atLimit}c";

    $topic = ResourceName::topic($atLimit);

    expect($topic)->toBe($atLimit);
    expect(fn (): string => ResourceName::topic($overLimit))
        ->toThrow(function (LengthException $exception) use ($overLimit): void {
            expect($exception->getMessage())
                ->toContain("Logical topic [$overLimit]")
                ->toContain('255-byte transport limit');
        });
});
