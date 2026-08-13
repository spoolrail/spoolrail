<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Outbox\OutboxPublication;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithOutbox;

uses(InteractsWithOutbox::class);

afterEach(function (): void {
    CarbonImmutable::setTestNow();
});

test('stages a normalized publication without resolving the broker driver', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'unavailable']);

    Spoolrail::extend(
        'unavailable',
        static fn (): never => throw new RuntimeException('The broker driver was resolved.'),
    );

    CarbonImmutable::setTestNow('2026-08-06 10:11:12.345999 UTC');
    $message = Message::make('order.created', ['order_id' => 42]);

    // --- Act ---
    $published = Spoolrail::connection('events')->publish(
        'orders',
        $message,
        ['correlation-id' => 'order-42'],
        orderingKey: 'order:42',
    );

    // --- Assert ---
    $row = DB::table('outbox_publications')->sole();
    $storedMessage = json_decode((string) $row->message, true, flags: JSON_THROW_ON_ERROR);
    $storedHeaders = json_decode((string) $row->headers, true, flags: JSON_THROW_ON_ERROR);
    $publication = OutboxPublication::query()->sole();

    expect($published->id)->toBe($message->id);
    expect($published->publishedAt?->format('Y-m-d H:i:s.u e'))
        ->toBe('2026-08-06 10:11:12.345000 UTC');
    expect($message->publishedAt)->toBeNull();
    expect($row->connection)->toBe('events');
    expect($row->topic)->toBe('orders');
    expect($row->ordering_key)->toBe('order:42');
    expect($storedMessage)->toBe([
        'id' => $published->id,
        'type' => 'order.created',
        'payload' => ['order_id' => 42],
        'published_at' => '2026-08-06T10:11:12.345Z',
    ]);
    expect($storedHeaders)->toBe(['correlation-id' => 'order-42']);
    expect($publication->message)->toBe($storedMessage);
    expect($publication->headers)->toBe($storedHeaders);
    expect($row->last_error)->toBeNull();
});

test('commits and rolls back publication intent with the configured database transaction', function (): void {
    // --- Arrange ---
    $connection = DB::connection();

    // --- Act ---
    $connection->beginTransaction();
    $rolledBack = Spoolrail::publish(
        'orders',
        Message::make('order.cancelled', ['order_id' => 41]),
    );
    $connection->rollBack();

    $connection->transaction(function (): void {
        Spoolrail::publish(
            'orders',
            Message::make('order.created', ['order_id' => 42]),
        );
    });

    // --- Assert ---
    $rows = DB::table('outbox_publications')->get();

    expect($rolledBack->publishedAt)->not->toBeNull();
    expect($rows)->toHaveCount(1);
    expect($rows->sole()->connection)->toBe('array');
    expect(json_decode((string) $rows->sole()->message, true, flags: JSON_THROW_ON_ERROR)['payload'])
        ->toBe(['order_id' => 42]);
});

test('uses the configured outbox connection independently of other database transactions', function (): void {
    // --- Arrange ---
    config()->set('database.connections.outbox', [
        'driver' => 'sqlite',
        'database' => ':memory:',
        'prefix' => '',
        'foreign_key_constraints' => true,
    ]);
    config()->set('spoolrail.outbox.connection', 'outbox');
    DB::purge('outbox');
    $this->migrateOutbox();

    // --- Act ---
    DB::connection()->beginTransaction();
    Spoolrail::publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
    );
    DB::connection()->rollBack();

    // --- Assert ---
    expect(DB::connection('outbox')->table('outbox_publications')->count())->toBe(1);
});

test('rejects an invalid outbox database connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.connection', '');

    // --- Act ---
    $publish = fn (): Message => Spoolrail::publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
    );

    // --- Assert ---
    expect($publish)->toThrow(
        InvalidConfigException::class,
        'Spoolrail outbox connection must be null or a non-empty Laravel database connection name.',
    );
});

test('publishes directly without an outbox table when the policy is disabled', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', false);
    config()->set('spoolrail.connections.direct', ['driver' => 'recording']);

    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->once()
        ->with('orders', Mockery::type('string'), [], null);
    $driver->allows('consume');

    Spoolrail::extend('recording', static fn (): Driver => $driver);
    Schema::drop('outbox_publications');

    // --- Act ---
    $published = Spoolrail::connection('direct')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
    );

    // --- Assert ---
    expect($published->publishedAt)->not->toBeNull();
});
