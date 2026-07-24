<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Illuminate\Contracts\Foundation\Application;
use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Exceptions\InvalidTopicException;
use Spoolrail\Spoolrail\Exceptions\MessageTooLargeException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageSerializer;

afterEach(function (): void {
    CarbonImmutable::setTestNow();
});

test('publishes a normalized message envelope through a custom raw driver', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.custom', [
        'driver' => 'custom',
    ]);

    $publishedTopic = '';
    $publishedBody = '';
    $driver = Mockery::mock(Driver::class);
    $driver->shouldReceive('publish')
        ->once()
        ->andReturnUsing(function (string $topic, string $body) use (&$publishedTopic, &$publishedBody): void {
            $publishedTopic = $topic;
            $publishedBody = $body;
        });

    Spoolrail::extend(
        'custom',
        fn (Application $app, array $config, string $name): Driver => $driver,
    );

    CarbonImmutable::setTestNow('2026-07-15 14:23:08.417999 UTC');
    $original = Message::make('order.created', ['reference' => 'A-42']);

    // --- Act ---
    $published = Spoolrail::connection('custom')->publish('orders', $original);

    // --- Assert ---
    expect($publishedTopic)->toBe('orders');
    expect($published->id)->toBe($original->id);
    expect($published->type)->toBe($original->type);
    expect($published->payload)->toBe($original->payload);
    expect($published->publishedAt?->format('Y-m-d H:i:s.u e'))->toBe('2026-07-15 14:23:08.417000 UTC');
    expect($original->publishedAt)->toBeNull();

    $sent = (new MessageSerializer)->deserialize($publishedBody);
    expect($sent)->toEqual($published);
});

test('restamps repeated publications of the same logical message', function (): void {
    // --- Arrange ---
    $original = Message::make('order.created', []);
    CarbonImmutable::setTestNow('2026-07-15 14:23:08.417999 UTC');

    // --- Act ---
    $first = Spoolrail::publish('orders', $original);

    CarbonImmutable::setTestNow('2026-07-15 14:24:09.123999 UTC');
    $second = Spoolrail::publish('orders', $original);

    // --- Assert ---
    expect($first->id)->toBe($original->id);
    expect($second->id)->toBe($original->id);
    expect($first->publishedAt?->format('Y-m-d H:i:s.u e'))->toBe('2026-07-15 14:23:08.417000 UTC');
    expect($second->publishedAt?->format('Y-m-d H:i:s.u e'))->toBe('2026-07-15 14:24:09.123000 UTC');
});

test('rejects payloads that cannot cross the JSON boundary before raw publication', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.custom', [
        'driver' => 'custom',
    ]);

    $driver = Mockery::mock(Driver::class);
    $driver->allows('publish');

    Spoolrail::extend(
        'custom',
        fn (Application $app, array $config, string $name): Driver => $driver,
    );

    // --- Act ---
    $exception = null;

    try {
        Spoolrail::connection('custom')->publish(
            'orders',
            Message::make('order.created', ['invalid' => NAN]),
        );
    } catch (JsonException $caught) {
        $exception = $caught;
    }

    // --- Assert ---
    expect($exception)->toBeInstanceOf(JsonException::class);
    $driver->shouldNotHaveReceived('publish');
});

test('accepts an envelope at the shared size limit and rejects the next byte before raw publication', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.custom', [
        'driver' => 'custom',
    ]);

    CarbonImmutable::setTestNow('2026-07-24 12:00:00.000000 UTC');

    $driver = Mockery::mock(Driver::class);
    $publishedBodies = [];
    $driver->shouldReceive('publish')
        ->once()
        ->andReturnUsing(function (string $topic, string $body) use (&$publishedBodies): void {
            $publishedBodies[] = $body;
        });

    Spoolrail::extend(
        'custom',
        fn (Application $app, array $config, string $name): Driver => $driver,
    );

    $message = Message::make('order.created', ['body' => '']);
    $stamped = $message->withPublishedAt(CarbonImmutable::now('UTC'));
    $emptyEnvelopeBytes = strlen((new MessageSerializer)->serialize($stamped));
    $atLimit = Message::make('order.created', [
        'body' => str_repeat('a', Connection::MAX_ENVELOPE_BYTES - $emptyEnvelopeBytes),
    ]);
    $overLimit = Message::make('order.created', [
        'body' => str_repeat('a', Connection::MAX_ENVELOPE_BYTES - $emptyEnvelopeBytes + 1),
    ]);

    // --- Act ---
    Spoolrail::connection('custom')->publish('orders', $atLimit);

    $failure = null;

    try {
        Spoolrail::connection('custom')->publish('orders', $overLimit);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect(strlen($publishedBodies[0]))->toBe(Connection::MAX_ENVELOPE_BYTES);
    expect($failure)->toBeInstanceOf(MessageTooLargeException::class);
    expect($failure?->getMessage())->toBe(
        'Serialized message envelope is 262145 bytes; Spoolrail accepts at most 262144 bytes.',
    );
});

test('rejects a non-portable topic before raw publication', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.custom', [
        'driver' => 'custom',
    ]);

    $driver = Mockery::mock(Driver::class);
    $driver->allows('publish');

    Spoolrail::extend(
        'custom',
        fn (Application $app, array $config, string $name): Driver => $driver,
    );

    // --- Act ---
    $failure = null;

    try {
        Spoolrail::connection('custom')->publish(
            'orders.created',
            Message::make('order.created', []),
        );
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidTopicException::class);
    $driver->shouldNotHaveReceived('publish');
});
