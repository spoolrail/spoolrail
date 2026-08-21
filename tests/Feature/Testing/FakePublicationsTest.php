<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use PHPUnit\Framework\AssertionFailedError;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;

afterEach(function (): void {
    CarbonImmutable::setTestNow();
});

test('records logical publications before outbox handling', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', true);
    CarbonImmutable::setTestNow('2026-08-21 12:34:56.789999 UTC');

    Spoolrail::fake();
    $message = Message::make('order.created', ['order_id' => 42]);

    // --- Act ---
    $published = Spoolrail::publish(
        'orders',
        $message,
        ['trace-id' => 'trace-42'],
    );

    // --- Assert ---
    expect($published)->not->toBe($message)
        ->and($published->publishedAt?->format('Y-m-d H:i:s.u e'))
        ->toBe('2026-08-21 12:34:56.789000 UTC');
    Spoolrail::assertPublished(
        'orders',
        'order.created',
        fn (array $payload, array $headers): bool => $payload['order_id'] === 42
            && $headers['trace-id'] === 'trace-42',
    );
});

test('matches publications by topic type payload headers and count', function (): void {
    // --- Arrange ---
    Spoolrail::fake();

    // --- Act ---
    Spoolrail::publish('orders', Message::make('order.created', ['order_id' => 42]));
    Spoolrail::publish('orders', Message::make('order.created', ['order_id' => 43]));
    Spoolrail::connection('partner')->publish(
        'returns',
        Message::make('order.returned', ['order_id' => 44]),
        ['reason' => 'damaged'],
    );

    // --- Assert ---
    Spoolrail::assertPublished('orders', 'order.created', 2);
    Spoolrail::assertPublished(
        'returns',
        'order.returned',
        fn (array $payload, array $headers): bool => $payload === ['order_id' => 44]
            && $headers === ['reason' => 'damaged'],
    );
    Spoolrail::assertNotPublished('orders', 'order.cancelled');
    Spoolrail::assertNotPublished(
        'orders',
        'order.created',
        fn (array $payload): bool => $payload['order_id'] === 99,
    );
});

test('asserts that nothing was published', function (): void {
    Spoolrail::fake();

    Spoolrail::assertNothingPublished();
});

test('rejects assertions that contradict recorded publications', function (): void {
    // --- Arrange ---
    Spoolrail::fake();

    Spoolrail::publish('orders', Message::make('order.created', ['order_id' => 42]));

    // --- Act & Assert ---
    expect(fn () => Spoolrail::assertPublished('returns', 'order.created'))
        ->toThrow(AssertionFailedError::class);
    expect(fn () => Spoolrail::assertPublished('orders', 'order.created', 2))
        ->toThrow(AssertionFailedError::class);
    expect(fn () => Spoolrail::assertPublished(
        'orders',
        'order.created',
        fn (array $payload): bool => $payload['order_id'] === 99,
    ))->toThrow(AssertionFailedError::class);
    expect(fn () => Spoolrail::assertNotPublished('orders', 'order.created'))
        ->toThrow(AssertionFailedError::class);
    expect(fn () => Spoolrail::assertNothingPublished())
        ->toThrow(AssertionFailedError::class);
});
