<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Schema;
use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Events\MessagePublicationFailed;
use Spoolrail\Spoolrail\Events\MessagePublished;
use Spoolrail\Spoolrail\Events\MessagePublishing;
use Spoolrail\Spoolrail\Events\MessageStaged;
use Spoolrail\Spoolrail\Events\MessageStaging;
use Spoolrail\Spoolrail\Events\MessageStagingFailed;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithOutbox;

uses(InteractsWithOutbox::class);

afterEach(function (): void {
    CarbonImmutable::setTestNow();
});

test('observes one publication lifecycle around retries and header transformation', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', false);
    config()->set('spoolrail.publisher_retries', [
        'times' => 1,
        'delay_milliseconds' => 0,
    ]);
    config()->set('spoolrail.connections.events', ['driver' => 'observed']);

    $timeline = [];
    $observed = [];
    $attempts = 0;
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->twice()
        ->andReturnUsing(function (string $topic, string $body, array $headers, ?string $orderingKey) use (&$attempts, &$timeline): void {
            $timeline[] = 'attempt';
            $attempts++;

            expect($headers)->toBe([
                'correlation-id' => 'order-42',
                'traceparent' => '00-trace-span-01',
            ]);

            if ($attempts === 1) {
                throw PublicationException::notSent(new RuntimeException('Broker unavailable.'));
            }
        });

    Spoolrail::extend('observed', static fn (): Driver => $driver);
    Spoolrail::transformHeadersUsing(
        static function (array $headers) use (&$timeline): array {
            $timeline[] = 'transform';
            $headers['traceparent'] = '00-trace-span-01';

            return $headers;
        },
    );

    Event::listen(MessagePublishing::class, static function (MessagePublishing $event) use (&$timeline, &$observed): void {
        $timeline[] = MessagePublishing::class;
        $observed[] = $event;
    });
    Event::listen(MessagePublished::class, static function (MessagePublished $event) use (&$timeline, &$observed): void {
        $timeline[] = MessagePublished::class;
        $observed[] = $event;
    });

    $message = Message::make('order.created', ['order_id' => 42]);

    // --- Act ---
    $published = Spoolrail::connection('events')->publish(
        'orders',
        $message,
        ['correlation-id' => 'order-42'],
        'order:42',
    );

    // --- Assert ---
    expect($timeline)->toBe([
        MessagePublishing::class,
        'transform',
        'attempt',
        'attempt',
        MessagePublished::class,
    ]);
    expect($observed[0]->connectionName)->toBe('events');
    expect($observed[0]->topic)->toBe('orders');
    expect($observed[0]->message)->toBe($published);
    expect($observed[0]->headers)->toBe(['correlation-id' => 'order-42']);
    expect($observed[0]->orderingKey)->toBe('order:42');
    expect($observed[1]->message)->toBe($published);
    expect($observed[1]->headers)->toBe([
        'correlation-id' => 'order-42',
        'traceparent' => '00-trace-span-01',
    ]);
});

test('reports one publication failure without allowing its observer to replace it', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', false);
    config()->set('spoolrail.publisher_retries.times', 0);
    config()->set('spoolrail.connections.events', ['driver' => 'failing']);

    $failure = PublicationException::notSent(new RuntimeException('Broker unavailable.'));
    $events = [];
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')->once()->andThrow($failure);

    Spoolrail::extend('failing', static fn (): Driver => $driver);

    Event::listen(MessagePublishing::class, static function (MessagePublishing $event) use (&$events): void {
        $events[] = $event;
    });
    Event::listen(MessagePublished::class, static function (MessagePublished $event) use (&$events): void {
        $events[] = $event;
    });
    Event::listen(MessagePublicationFailed::class, static function (MessagePublicationFailed $event) use (&$events): never {
        $events[] = $event;

        throw new RuntimeException('Observer failed.');
    });

    // --- Act ---
    $caught = null;

    try {
        Spoolrail::connection('events')->publish(
            'orders',
            Message::make('order.created', ['order_id' => 42]),
        );
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
    expect(array_map(static fn (object $event): string => $event::class, $events))->toBe([
        MessagePublishing::class,
        MessagePublicationFailed::class,
    ]);
    expect($events[1]->exception)->toBe($failure);
});

test('contains before and terminal observers without changing a successful publication', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', false);
    config()->set('spoolrail.connections.events', ['driver' => 'successful']);

    $publishingObserved = false;
    $publishedObserved = false;
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->once()
        ->with('orders', Mockery::type('string'), ['correlation-id' => 'order-42'], null);

    Spoolrail::extend('successful', static fn (): Driver => $driver);

    Event::listen(MessagePublishing::class, static function () use (&$publishingObserved): never {
        $publishingObserved = true;

        throw new RuntimeException('Before observer failed.');
    });
    Event::listen(MessagePublished::class, static function () use (&$publishedObserved): never {
        $publishedObserved = true;

        throw new RuntimeException('Terminal observer failed.');
    });

    // --- Act ---
    $published = Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
        ['correlation-id' => 'order-42'],
    );

    // --- Assert ---
    expect($published->publishedAt)->not->toBeNull();
    expect($publishingObserved)->toBeTrue();
    expect($publishedObserved)->toBeTrue();
});

test('replaces the transformer for an already resolved connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', false);
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);

    $publishedHeaders = null;
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->once()
        ->andReturnUsing(static function (string $topic, string $body, array $headers) use (&$publishedHeaders): void {
            $publishedHeaders = $headers;
        });

    Spoolrail::extend('recording', static fn (): Driver => $driver);
    $connection = Spoolrail::connection('events');
    Spoolrail::transformHeadersUsing(
        static fn (array $headers): array => [...$headers, 'first-transformer' => 'used'],
    );
    Spoolrail::transformHeadersUsing(
        static fn (array $headers): array => [...$headers, 'second-transformer' => 'used'],
    );

    // --- Act ---
    $connection->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
        ['correlation-id' => 'order-42'],
    );

    // --- Assert ---
    expect($publishedHeaders)->toBe([
        'correlation-id' => 'order-42',
        'second-transformer' => 'used',
    ]);
});

test('discards throwing non-array and non-portable transformer candidates as a whole', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', false);
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);

    $publishedHeaders = [];
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->times(3)
        ->andReturnUsing(static function (string $topic, string $body, array $headers) use (&$publishedHeaders): void {
            $publishedHeaders[] = $headers;
        });

    Spoolrail::extend('recording', static fn (): Driver => $driver);
    $connection = Spoolrail::connection('events');
    $message = Message::make('order.created', ['order_id' => 42]);
    $original = ['correlation-id' => 'order-42'];

    // --- Act ---
    Spoolrail::transformHeadersUsing(
        static fn (array $headers): never => throw new RuntimeException('Transformer failed.'),
    );
    $connection->publish('orders', $message, $original);

    Spoolrail::transformHeadersUsing(static fn (array $headers): string => 'invalid');
    $connection->publish('orders', $message, $original);

    Spoolrail::transformHeadersUsing(
        static fn (array $headers): array => [...$headers, 'Invalid-Header' => 'value'],
    );
    $connection->publish('orders', $message, $original);

    // --- Assert ---
    expect($publishedHeaders)->toBe([$original, $original, $original]);
});

test('discards a transformer candidate that makes the complete publication oversized', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', false);
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);
    CarbonImmutable::setTestNow('2026-08-31 12:00:00.000000 UTC');

    $envelope = new MessageEnvelope;
    $originalHeaders = ['traceparent' => 'trace'];
    $empty = Message::make('order.created', ['body' => '']);
    $stamped = $empty->withPublishedAt(CarbonImmutable::now('UTC'));
    $originalHeaderBytes = strlen('traceparent') + strlen('trace') + strlen('String');
    $message = Message::make('order.created', [
        'body' => str_repeat(
            'a',
            Connection::MAX_PUBLICATION_BYTES - strlen($envelope->encode($stamped)) - $originalHeaderBytes,
        ),
    ]);

    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->once()
        ->with('orders', Mockery::type('string'), $originalHeaders, null);

    Spoolrail::extend('recording', static fn (): Driver => $driver);
    Spoolrail::transformHeadersUsing(
        static fn (array $headers): array => [...$headers, 'tracestate' => 'vendor=state'],
    );

    // --- Act ---
    Spoolrail::connection('events')->publish('orders', $message, $originalHeaders);
});

test('observes staging and later publication while transforming headers only before storage', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);

    $timeline = [];
    $observed = [];
    $transformations = 0;
    $publishedHeaders = null;
    $driver = Mockery::mock(Driver::class);
    $driver->expects('publish')
        ->once()
        ->andReturnUsing(static function (string $topic, string $body, array $headers) use (&$publishedHeaders): void {
            $publishedHeaders = $headers;
        });

    Spoolrail::extend('recording', static fn (): Driver => $driver);
    Spoolrail::transformHeadersUsing(
        static function (array $headers) use (&$transformations): array {
            $transformations++;
            $headers['traceparent'] = '00-trace-span-01';

            return $headers;
        },
    );

    foreach ([MessageStaging::class, MessageStaged::class, MessagePublishing::class, MessagePublished::class] as $eventClass) {
        Event::listen($eventClass, static function (object $event) use (&$timeline, &$observed): void {
            $timeline[] = $event::class;
            $observed[] = $event;
        });
    }

    // --- Act ---
    $staged = Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
        ['correlation-id' => 'order-42'],
        'order:42',
    );
    $exitCode = $this->artisan('spoolrail:publish')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($timeline)->toBe([
        MessageStaging::class,
        MessageStaged::class,
        MessagePublishing::class,
        MessagePublished::class,
    ]);
    expect($transformations)->toBe(1);
    expect($observed[0]->message)->toBe($staged);
    expect($observed[0]->headers)->toBe(['correlation-id' => 'order-42']);
    expect($observed[1]->headers)->toBe([
        'correlation-id' => 'order-42',
        'traceparent' => '00-trace-span-01',
    ]);
    expect($observed[1]->outboxId)->toBeInt();
    expect($observed[2]->message)->toEqual($staged);
    expect($observed[2]->headers)->toBe($observed[1]->headers);
    expect($observed[3]->headers)->toBe($observed[1]->headers);
    expect($publishedHeaders)->toBe($observed[1]->headers);
    expect(DB::table('outbox_publications')->count())->toBe(0);
});

test('reports one staging failure without allowing its observer to replace it', function (): void {
    // --- Arrange ---
    $events = [];
    Schema::drop('outbox_publications');

    Event::listen(MessageStaging::class, static function (MessageStaging $event) use (&$events): void {
        $events[] = $event;
    });
    Event::listen(MessageStaged::class, static function (MessageStaged $event) use (&$events): void {
        $events[] = $event;
    });
    Event::listen(MessageStagingFailed::class, static function (MessageStagingFailed $event) use (&$events): never {
        $events[] = $event;

        throw new RuntimeException('Observer failed.');
    });

    // --- Act ---
    $caught = null;

    try {
        Spoolrail::publish(
            'orders',
            Message::make('order.created', ['order_id' => 42]),
        );
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->not->toBeNull();
    expect(array_map(static fn (object $event): string => $event::class, $events))->toBe([
        MessageStaging::class,
        MessageStagingFailed::class,
    ]);
    expect($events[1]->exception)->toBe($caught);
});

test('emits no direct publication lifecycle before publication validation succeeds', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', false);
    $events = [];

    foreach ([MessagePublishing::class, MessagePublished::class, MessagePublicationFailed::class] as $eventClass) {
        Event::listen($eventClass, static function (object $event) use (&$events): void {
            $events[] = $event;
        });
    }

    // --- Act ---
    try {
        Spoolrail::publish('invalid.topic', Message::make('order.created', []));
    } catch (InvalidArgumentException) {
    }

    // --- Assert ---
    expect($events)->toBe([]);
});

test('emits no direct publication lifecycle before retry configuration is valid', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', false);
    config()->set('spoolrail.publisher_retries.times', -1);
    $events = [];

    foreach ([MessagePublishing::class, MessagePublished::class, MessagePublicationFailed::class] as $eventClass) {
        Event::listen($eventClass, static function (object $event) use (&$events): void {
            $events[] = $event;
        });
    }

    // --- Act ---
    try {
        Spoolrail::publish('orders', Message::make('order.created', []));
    } catch (InvalidArgumentException) {
    }

    // --- Assert ---
    expect($events)->toBe([]);
});
