<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Illuminate\Support\Sleep;
use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Spoolrail\Spoolrail\Exceptions\MessageTooLargeException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\TransportContext;

afterEach(function (): void {
    CarbonImmutable::setTestNow();
    Sleep::fake(false);
});

test('retries the same prepared publication', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.publisher_retries', [
        'times' => 2,
        'delay_milliseconds' => 25,
    ]);
    Sleep::fake();
    CarbonImmutable::setTestNow('2026-07-15 14:23:08.417999 UTC');

    $attempts = [];
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->times(3)
        ->andReturnUsing(function (string $topic, string $body, array $headers, ?string $orderingKey) use (&$attempts): void {
            $attempts[] = [$topic, $body, $headers, $orderingKey];

            if (count($attempts) === 1) {
                throw PublicationException::notSent(new RuntimeException('Connection refused.'));
            }

            if (count($attempts) === 2) {
                throw new RuntimeException('Transient driver failure.');
            }
        });
    $connection = new Connection($driver, new MessageEnvelope);
    $message = Message::make('order.created', ['reference' => 'A-42']);

    // --- Act ---
    $published = $connection->publish(
        'orders',
        $message,
        ['correlation-id' => 'A-42'],
        'order:42',
    );

    // --- Assert ---
    expect($published->id)->toBe($message->id);
    expect($attempts)->toHaveCount(3);
    expect($attempts[1])->toBe($attempts[0]);
    expect($attempts[2])->toBe($attempts[0]);
    Sleep::assertSequence([
        Sleep::for(25)->milliseconds(),
        Sleep::for(25)->milliseconds(),
    ]);
});

test('does not retry an explicit publication rejection', function (): void {
    // --- Arrange ---
    Sleep::fake();
    $failure = PublicationException::rejected();
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')->once()->andThrow($failure);
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $caught = null;

    try {
        $connection->publish('orders', Message::make('order.created', []));
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
    Sleep::assertNeverSlept();
});

test('allows publication retries to be disabled', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.publisher_retries.times', 0);
    $failure = PublicationException::notSent(new RuntimeException('Connection refused.'));
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')->once()->andThrow($failure);
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $caught = null;

    try {
        $connection->publish('orders', Message::make('order.created', []));
    } catch (PublicationException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
});

test('rejects an invalid publisher retry setting before broker I/O', function (string $setting, mixed $value): void {
    config()->set("spoolrail.publisher_retries.$setting", $value);
    $driver = Mockery::mock(Driver::class);
    $driver->shouldNotReceive('publish');
    $connection = new Connection($driver, new MessageEnvelope);

    expect(fn (): Message => $connection->publish('orders', Message::make('order.created', [])))
        ->toThrow(
            InvalidArgumentException::class,
            "Spoolrail publisher retry setting [$setting] must be a non-negative integer.",
        );
})->with([
    'negative retry count' => ['times', -1],
    'non-integer delay' => ['delay_milliseconds', '1000'],
]);

test('retains an unknown outcome when a later attempt conclusively fails', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.publisher_retries', [
        'times' => 1,
        'delay_milliseconds' => 0,
    ]);
    $ambiguous = PublicationException::outcomeUnknown(new RuntimeException('Response lost.'));
    $rejected = PublicationException::rejected();
    $attempt = 0;
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->twice()
        ->andReturnUsing(function () use (&$attempt, $ambiguous, $rejected): never {
            $attempt++;

            throw $attempt === 1 ? $ambiguous : $rejected;
        });
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $caught = null;

    try {
        $connection->publish('orders', Message::make('order.created', []));
    } catch (PublicationException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($ambiguous);
    expect($caught?->outcome)->toBe(PublicationOutcome::Unknown);
});

test('reports the final conclusive failure after retries are exhausted', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.publisher_retries', [
        'times' => 1,
        'delay_milliseconds' => 0,
    ]);
    $firstFailure = PublicationException::notSent(new RuntimeException('Connection refused.'));
    $finalFailure = PublicationException::notSent(new RuntimeException('Service unavailable.'));
    $attempt = 0;
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->twice()
        ->andReturnUsing(function () use (&$attempt, $firstFailure, $finalFailure): never {
            $attempt++;

            throw $attempt === 1 ? $firstFailure : $finalFailure;
        });
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $caught = null;

    try {
        $connection->publish('orders', Message::make('order.created', []));
    } catch (PublicationException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($finalFailure);
});

test('closes a driver that owns external resources', function (): void {
    // --- Arrange ---
    $driver = new class implements CanClose, Driver
    {
        public bool $closed = false;

        public function publish(
            string $topic,
            string $body,
            array $headers,
            ?string $orderingKey = null,
        ): void {}

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
    $publishedHeaders = [];
    $driver = Mockery::mock(Driver::class);
    $driver->shouldReceive('publish')
        ->once()
        ->andReturnUsing(function (string $topic, string $body, array $headers) use (&$publishedTopic, &$publishedBody, &$publishedHeaders): void {
            $publishedTopic = $topic;
            $publishedBody = $body;
            $publishedHeaders = $headers;
        });
    $envelope = new MessageEnvelope;
    $connection = new Connection($driver, $envelope);

    CarbonImmutable::setTestNow('2026-07-15 14:23:08.417999 UTC');
    $original = Message::make('order.created', ['reference' => 'A-42']);

    // --- Act ---
    $published = $connection->publish(
        'orders',
        $original,
        ['correlation-id' => 'A-42'],
    );

    // --- Assert ---
    expect($publishedTopic)->toBe('orders');
    expect($published->id)->toBe($original->id);
    expect($published->type)->toBe($original->type);
    expect($published->payload)->toBe($original->payload);
    expect($published->publishedAt?->format('Y-m-d H:i:s.u e'))->toBe('2026-07-15 14:23:08.417000 UTC');
    expect($original->publishedAt)->toBeNull();
    expect($published->transport)->toBeNull();
    expect($publishedHeaders)->toBe(['correlation-id' => 'A-42']);
    expect($envelope->decode($publishedBody))->toEqual($published);
});

test('clears inbound transport context from a newly published copy', function (): void {
    // --- Arrange ---
    $transport = new TransportContext(
        driver: 'array',
        connectionName: 'array',
        topic: 'orders',
        subscription: 'warehouse-orders',
        headers: ['correlation-id' => 'inbound'],
        redelivered: true,
    );
    $received = Message::make('order.created', [])
        ->withPublishedAt(CarbonImmutable::parse('2026-07-15 14:23:08.417 UTC'))
        ->withTransport($transport);
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->once()
        ->with('returns', Mockery::type('string'), [], null);
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $republished = $connection->publish('returns', $received);

    // --- Assert ---
    expect($received->transport)->toBe($transport);
    expect($republished->id)->toBe($received->id);
    expect($republished->transport)->toBeNull();
});

test('passes a portable ordering key through the raw driver', function (): void {
    // --- Arrange ---
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->once()
        ->with('orders', Mockery::type('string'), [], 'order:42');
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $connection->publish(
        'orders',
        Message::make('order.created', []),
        orderingKey: 'order:42',
    );
});

test('accepts an ordering key at the 128-character portable limit', function (): void {
    // --- Arrange ---
    $orderingKey = str_repeat('o', 128);
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->once()
        ->with('orders', Mockery::type('string'), [], $orderingKey);
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $connection->publish(
        'orders',
        Message::make('order.created', []),
        orderingKey: $orderingKey,
    );
});

test('rejects a non-portable ordering key before driver I/O', function (string $orderingKey): void {
    $driver = Mockery::mock(Driver::class);
    $driver->shouldNotReceive('publish');
    $connection = new Connection($driver, new MessageEnvelope);

    expect(fn (): Message => $connection->publish(
        'orders',
        Message::make('order.created', []),
        orderingKey: $orderingKey,
    ))->toThrow(
        InvalidArgumentException::class,
        'The ordering key must contain between 1 and 128 printable ASCII characters without spaces.',
    );
})->with([
    'empty' => '',
    'space' => 'order 42',
    'non-ASCII' => 'order:é',
    'over 128 characters' => str_repeat('o', 129),
]);

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

test('accepts a complete publication at the shared size limit and rejects the next byte', function (): void {
    // --- Arrange ---
    CarbonImmutable::setTestNow('2026-07-24 12:00:00.000000 UTC');

    $publishedBodies = [];
    $headers = ['traceparent' => 'trace'];
    $driver = Mockery::mock(Driver::class);
    $driver->shouldReceive('publish')
        ->once()
        ->andReturnUsing(function (string $topic, string $body, array $publishedHeaders) use (&$publishedBodies, $headers): void {
            expect($publishedHeaders)->toBe($headers);
            $publishedBodies[] = $body;
        });
    $envelope = new MessageEnvelope;
    $connection = new Connection($driver, $envelope);

    $message = Message::make('order.created', ['body' => '']);
    $stamped = $message->withPublishedAt(CarbonImmutable::now('UTC'));
    $emptyEnvelopeBytes = strlen($envelope->encode($stamped));
    $headerBytes = strlen('traceparent') + strlen('trace') + strlen('String');
    $atLimit = Message::make('order.created', [
        'body' => str_repeat(
            'a',
            Connection::MAX_PUBLICATION_BYTES - $emptyEnvelopeBytes - $headerBytes,
        ),
    ]);
    $overLimit = Message::make('order.created', [
        'body' => str_repeat(
            'a',
            Connection::MAX_PUBLICATION_BYTES - $emptyEnvelopeBytes - $headerBytes + 1,
        ),
    ]);
    $rejectOverLimit = fn (): Message => $connection->publish('orders', $overLimit, $headers);

    // --- Act ---
    $connection->publish('orders', $atLimit, $headers);

    // --- Assert ---
    expect(strlen($publishedBodies[0]) + $headerBytes)
        ->toBe(Connection::MAX_PUBLICATION_BYTES);
    expect($rejectOverLimit)
        ->toThrow(function (MessageTooLargeException $exception): void {
            expect($exception->bytes)->toBe(262_145);
            expect($exception->limit)->toBe(Connection::MAX_PUBLICATION_BYTES);
            expect($exception->getMessage())->toBe(
                'Serialized message publication is 262145 bytes; Spoolrail accepts at most 262144 bytes.',
            );
        });
});

test('accepts ten headers at their portable key and value boundaries', function (): void {
    // --- Arrange ---
    $headers = [
        str_repeat('a', 128) => str_repeat('é', 512),
        'header-2' => 'value-2',
        'header-3' => 'value-3',
        'header-4' => 'value-4',
        'header-5' => 'value-5',
        'header-6' => 'value-6',
        'header-7' => 'value-7',
        'header-8' => 'value-8',
        'header-9' => 'value-9',
        'header-10' => 'value-10',
    ];
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->once()
        ->with('orders', Mockery::type('string'), $headers, null);
    $connection = new Connection($driver, new MessageEnvelope);

    // --- Act ---
    $connection->publish('orders', Message::make('order.created', []), $headers);

    // --- Assert ---
    expect($headers)->toHaveCount(10);
    expect(strlen(array_key_first($headers)))->toBe(128);
    expect(strlen($headers[array_key_first($headers)]))->toBe(1_024);
});

test('rejects an eleventh header before driver I/O', function (): void {
    $driver = Mockery::mock(Driver::class);
    $driver->shouldNotReceive('publish');
    $connection = new Connection($driver, new MessageEnvelope);
    $headers = array_fill_keys(
        array_map(static fn (int $number): string => "header-$number", range(1, 11)),
        'value',
    );

    expect(fn (): Message => $connection->publish(
        'orders',
        Message::make('order.created', []),
        $headers,
    ))->toThrow(
        InvalidArgumentException::class,
        'A message publication may contain at most 10 headers.',
    );
});

test('rejects header keys outside the portable grammar before driver I/O', function (array $headers): void {
    $driver = Mockery::mock(Driver::class);
    $driver->shouldNotReceive('publish');
    $connection = new Connection($driver, new MessageEnvelope);

    expect(fn (): Message => $connection->publish(
        'orders',
        Message::make('order.created', []),
        $headers,
    ))->toThrow(
        InvalidArgumentException::class,
        'Message header keys must begin with a lowercase letter',
    );
})->with([
    'integer key' => [[0 => 'value']],
    'uppercase letter' => [['Correlation-Id' => 'value']],
    'leading digit' => [['1-correlation-id' => 'value']],
    'leading hyphen' => [['-correlation-id' => 'value']],
    'trailing hyphen' => [['correlation-id-' => 'value']],
    'adjacent hyphens' => [['correlation--id' => 'value']],
    'underscore' => [['correlation_id' => 'value']],
]);

test('rejects a header key over 128 ASCII bytes before driver I/O', function (): void {
    $driver = Mockery::mock(Driver::class);
    $driver->shouldNotReceive('publish');
    $connection = new Connection($driver, new MessageEnvelope);
    $key = str_repeat('a', 129);

    expect(fn (): Message => $connection->publish(
        'orders',
        Message::make('order.created', []),
        [$key => 'value'],
    ))->toThrow(
        InvalidArgumentException::class,
        "Message header [$key] exceeds the 128-byte limit.",
    );
});

test('rejects header values outside the portable string contract before driver I/O', function (mixed $value, string $message): void {
    $driver = Mockery::mock(Driver::class);
    $driver->shouldNotReceive('publish');
    $connection = new Connection($driver, new MessageEnvelope);

    expect(fn (): Message => $connection->publish(
        'orders',
        Message::make('order.created', []),
        ['correlation-id' => $value],
    ))->toThrow(InvalidArgumentException::class, $message);
})->with([
    'non-string' => [42, 'must have a string value'],
    'empty' => ['', 'must have a non-empty valid UTF-8 value'],
    'invalid UTF-8' => ["\xFF", 'must have a non-empty valid UTF-8 value'],
    'over 1024 UTF-8 bytes' => [str_repeat('é', 513), 'exceeds the 1024-byte value limit'],
]);

test('accepts a topic at the portable limit and rejects the next character before raw publication', function (): void {
    // --- Arrange ---
    $topicAtLimit = 't'.str_repeat('o', 250);
    $topicOverLimit = "{$topicAtLimit}o";
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')->once()->with($topicAtLimit, Mockery::type('string'), [], null);
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
