<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Exceptions\InvalidTopicException;
use Spoolrail\Spoolrail\Exceptions\MessageTooLargeException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageSerializer;

afterEach(function (): void {
    CarbonImmutable::setTestNow();
});

test('publishes a normalized message envelope through the raw driver', function (): void {
    // --- Arrange ---
    $publishedTopic = '';
    $publishedBody = '';
    $driver = Mockery::mock(Driver::class);
    $driver->shouldReceive('publish')
        ->once()
        ->andReturnUsing(function (string $topic, string $body) use (&$publishedTopic, &$publishedBody): void {
            $publishedTopic = $topic;
            $publishedBody = $body;
        });
    $serializer = new MessageSerializer;
    $connection = new Connection($driver, $serializer);

    CarbonImmutable::setTestNow('2026-07-15 14:23:08.417999 UTC');
    $original = Message::make('order.created', ['reference' => 'A-42']);

    // --- Act ---
    $published = $connection->publish('orders', $original);

    // --- Assert ---
    expect($publishedTopic)->toBe('orders');
    expect($published->id)->toBe($original->id);
    expect($published->type)->toBe($original->type);
    expect($published->payload)->toBe($original->payload);
    expect($published->publishedAt?->format('Y-m-d H:i:s.u e'))->toBe('2026-07-15 14:23:08.417000 UTC');
    expect($original->publishedAt)->toBeNull();
    expect($serializer->deserialize($publishedBody))->toEqual($published);
});

test('restamps repeated publications of the same logical message', function (): void {
    // --- Arrange ---
    $driver = Mockery::mock(Driver::class);
    $driver->shouldReceive('publish')->twice();
    $connection = new Connection($driver, new MessageSerializer);
    $original = Message::make('order.created', []);
    CarbonImmutable::setTestNow('2026-07-15 14:23:08.417999 UTC');

    // --- Act ---
    $first = $connection->publish('orders', $original);

    CarbonImmutable::setTestNow('2026-07-15 14:24:09.123999 UTC');
    $second = $connection->publish('orders', $original);

    // --- Assert ---
    expect($first->id)->toBe($original->id);
    expect($second->id)->toBe($original->id);
    expect($first->publishedAt?->format('Y-m-d H:i:s.u e'))->toBe('2026-07-15 14:23:08.417000 UTC');
    expect($second->publishedAt?->format('Y-m-d H:i:s.u e'))->toBe('2026-07-15 14:24:09.123000 UTC');
});

test('rejects payloads that cannot cross the JSON boundary before raw publication', function (): void {
    // --- Arrange ---
    $driver = Mockery::mock(Driver::class);
    $driver->allows('publish');
    $connection = new Connection($driver, new MessageSerializer);

    // --- Act ---
    $action = fn (): Message => $connection->publish(
        'orders',
        Message::make('order.created', ['invalid' => NAN]),
    );

    // --- Assert ---
    expect($action)->toThrow(JsonException::class);
    $driver->shouldNotHaveReceived('publish');
});

test('accepts an envelope at the shared size limit and rejects the next byte', function (): void {
    // --- Arrange ---
    CarbonImmutable::setTestNow('2026-07-24 12:00:00.000000 UTC');

    $publishedBodies = [];
    $driver = Mockery::mock(Driver::class);
    $driver->shouldReceive('publish')
        ->once()
        ->andReturnUsing(function (string $topic, string $body) use (&$publishedBodies): void {
            $publishedBodies[] = $body;
        });
    $serializer = new MessageSerializer;
    $connection = new Connection($driver, $serializer);

    $message = Message::make('order.created', ['body' => '']);
    $stamped = $message->withPublishedAt(CarbonImmutable::now('UTC'));
    $emptyEnvelopeBytes = strlen($serializer->serialize($stamped));
    $atLimit = Message::make('order.created', [
        'body' => str_repeat('a', Connection::MAX_ENVELOPE_BYTES - $emptyEnvelopeBytes),
    ]);
    $overLimit = Message::make('order.created', [
        'body' => str_repeat('a', Connection::MAX_ENVELOPE_BYTES - $emptyEnvelopeBytes + 1),
    ]);
    $rejectOverLimit = fn (): Message => $connection->publish('orders', $overLimit);

    // --- Act ---
    $connection->publish('orders', $atLimit);

    // --- Assert ---
    expect(strlen($publishedBodies[0]))->toBe(Connection::MAX_ENVELOPE_BYTES);
    expect($rejectOverLimit)
        ->toThrow(
            MessageTooLargeException::class,
            'Serialized message envelope is 262145 bytes; Spoolrail accepts at most 262144 bytes.',
        );
});

test('rejects a non-portable topic before raw publication', function (): void {
    // --- Arrange ---
    $driver = Mockery::mock(Driver::class);
    $driver->allows('publish');
    $connection = new Connection($driver, new MessageSerializer);

    // --- Act ---
    $action = fn (): Message => $connection->publish(
        'orders.created',
        Message::make('order.created', []),
    );

    // --- Assert ---
    expect($action)->toThrow(InvalidTopicException::class);
    $driver->shouldNotHaveReceived('publish');
});
