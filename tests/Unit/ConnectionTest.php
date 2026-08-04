<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Exceptions\MessageTooLargeException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;

afterEach(function (): void {
    CarbonImmutable::setTestNow();
});

test('closes a driver that owns external resources', function (): void {
    // --- Arrange ---
    $driver = new class implements CanClose, Driver
    {
        public bool $closed = false;

        public function publish(string $topic, string $body): void {}

        public function consume(string $subscription, Closure $handoff): void {}

        public function close(): void
        {
            $this->closed = true;
        }
    };
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $connection->close();

    // --- Assert ---
    expect($driver->closed)->toBeTrue();
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
    $envelope = new MessageEnvelope;
    $connection = new Connection($driver, $envelope);

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
    expect($envelope->decode($publishedBody))->toEqual($published);
});

test('restamps repeated publications of the same logical message', function (): void {
    // --- Arrange ---
    $driver = Mockery::mock(Driver::class);
    $driver->shouldReceive('publish')->twice();
    $connection = new Connection($driver, new MessageEnvelope);
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
    $connection = new Connection($driver, new MessageEnvelope);

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
    $envelope = new MessageEnvelope;
    $connection = new Connection($driver, $envelope);

    $message = Message::make('order.created', ['body' => '']);
    $stamped = $message->withPublishedAt(CarbonImmutable::now('UTC'));
    $emptyEnvelopeBytes = strlen($envelope->encode($stamped));
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
        ->toThrow(function (MessageTooLargeException $exception): void {
            expect($exception->bytes)->toBe(262_145);
            expect($exception->limit)->toBe(Connection::MAX_ENVELOPE_BYTES);
            expect($exception->getMessage())->toBe(
                'Serialized message envelope is 262145 bytes; Spoolrail accepts at most 262144 bytes.',
            );
        });
});

test('accepts a topic at the portable limit and rejects the next character before raw publication', function (): void {
    // --- Arrange ---
    $topicAtLimit = 't'.str_repeat('o', 250);
    $topicOverLimit = "{$topicAtLimit}o";
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')->once()->with($topicAtLimit, Mockery::type('string'));
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $connection->publish($topicAtLimit, Message::make('order.created', []));
    $rejectOverLimit = fn (): Message => $connection->publish(
        $topicOverLimit,
        Message::make('order.created', []),
    );

    // --- Assert ---
    expect($rejectOverLimit)->toThrow(
        InvalidArgumentException::class,
        "Topic [$topicOverLimit] must contain between 3 and 251 ASCII characters",
    );
});

test('rejects a non-portable subscription before raw consumption', function (): void {
    $driver = Mockery::mock(Driver::class);
    $driver->shouldNotReceive('consume');
    $connection = new Connection($driver, new MessageEnvelope);

    expect(fn () => $connection->consume(
        's'.str_repeat('u', 50),
        static function (): void {},
    ))->toThrow(InvalidArgumentException::class);
});
