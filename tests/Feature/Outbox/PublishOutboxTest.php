<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Illuminate\Cache\RateLimiter;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Spoolrail\Spoolrail\Exceptions\OutboxPublicationException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Outbox\PublishOutbox;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithOutbox;

uses(InteractsWithOutbox::class);

afterEach(function (): void {
    CarbonImmutable::setTestNow();
});

test('publishes committed rows and removes them after broker acceptance', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);

    $publications = [];
    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->shouldReceive('publish')
        ->twice()
        ->andReturnUsing(function (string $topic, string $body, array $headers) use (&$publications): void {
            $publications[] = [
                'topic' => $topic,
                'message' => json_decode($body, true, flags: JSON_THROW_ON_ERROR),
                'headers' => $headers,
            ];
        });

    $creations = 0;
    Spoolrail::extend('recording', function () use ($driver, &$creations): Driver {
        $creations++;

        return $driver;
    });

    $first = Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 41]),
    );
    $second = Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
        ['correlation-id' => 'order-42'],
    );

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:outbox:publish')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($publications)->toBe([
        [
            'topic' => 'orders',
            'message' => [
                'id' => $first->id,
                'type' => 'order.created',
                'payload' => ['order_id' => 41],
                'published_at' => $first->publishedAt?->format('Y-m-d\TH:i:s.v\Z'),
            ],
            'headers' => [],
        ],
        [
            'topic' => 'orders',
            'message' => [
                'id' => $second->id,
                'type' => 'order.created',
                'payload' => ['order_id' => 42],
                'published_at' => $second->publishedAt?->format('Y-m-d\TH:i:s.v\Z'),
            ],
            'headers' => ['correlation-id' => 'order-42'],
        ],
    ]);
    expect($creations)->toBe(1);
    expect(DB::table('outbox_publications')->count())->toBe(0);
});

test('finishes the current publication before stopping', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'stoppable']);

    $attempts = [];
    $publishOutbox = app(PublishOutbox::class);
    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->shouldReceive('publish')
        ->andReturnUsing(function (string $topic, string $body) use (&$attempts, $publishOutbox): void {
            $message = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
            $attempts[] = [$topic, $message['payload']['sequence']];
            $publishOutbox->stop();
        });

    Spoolrail::extend('stoppable', static fn (): Driver => $driver);

    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'first']),
    );
    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'second']),
    );

    // --- Act ---
    $succeeded = $publishOutbox();

    // --- Assert ---
    $pending = DB::table('outbox_publications')->orderBy('id')->get();

    expect($succeeded)->toBeTrue();
    expect($attempts)->toBe([['orders', 'first']]);
    expect($pending)->toHaveCount(1);
    expect(json_decode((string) $pending[0]->message, true, flags: JSON_THROW_ON_ERROR)['payload']['sequence'])
        ->toBe('second');
});

test('records a failed current publication before stopping', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'stoppable']);

    $attempts = [];
    $publishOutbox = app(PublishOutbox::class);
    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->shouldReceive('publish')
        ->andReturnUsing(function (string $topic, string $body) use (&$attempts, $publishOutbox): never {
            $message = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
            $attempts[] = [$topic, $message['payload']['sequence']];
            $publishOutbox->stop();

            throw PublicationException::notSent(new RuntimeException('Broker unavailable.'));
        });

    Spoolrail::extend('stoppable', static fn (): Driver => $driver);

    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'failed']),
    );
    Spoolrail::connection('events')->publish(
        'returns',
        Message::make('return.created', ['sequence' => 'unattempted']),
    );

    // --- Act ---
    $succeeded = $publishOutbox();

    // --- Assert ---
    $pending = DB::table('outbox_publications')->orderBy('id')->get();

    expect($succeeded)->toBeFalse();
    expect($attempts)->toBe([['orders', 'failed']]);
    expect($pending)->toHaveCount(2);
    expect($pending[0]->last_error)->toBe('The publication failed before the message was sent.');
    expect($pending[1]->last_error)->toBeNull();
});

test('blocks a failed lane while publishing unrelated lanes and returns failure', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'selective']);

    $attempts = [];
    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->shouldReceive('publish')
        ->andReturnUsing(function (string $topic, string $body) use (&$attempts): void {
            $message = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
            $attempts[] = [$topic, $message['payload']['sequence']];

            if ($message['payload']['sequence'] === 'orders-1') {
                throw PublicationException::notSent(new RuntimeException('Broker unavailable.'));
            }
        });

    Spoolrail::extend('selective', static fn (): Driver => $driver);

    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'orders-1']),
    );
    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'orders-2']),
    );
    Spoolrail::connection('events')->publish(
        'returns',
        Message::make('return.created', ['sequence' => 'returns-1']),
    );

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:outbox:publish')->run();

    // --- Assert ---
    $pending = DB::table('outbox_publications')->orderBy('id')->get();

    expect($exitCode)->toBe(1);
    expect($attempts)->toBe([
        ['orders', 'orders-1'],
        ['returns', 'returns-1'],
    ]);
    expect($pending)->toHaveCount(2);
    expect($pending[0]->last_error)->toBe('The publication failed before the message was sent.');
    expect($pending[1]->last_error)->toBeNull();
});

test('attempts fresh lane heads before retrying known failures', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'selective']);

    $attempts = [];
    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->shouldReceive('publish')
        ->andReturnUsing(function (string $topic, string $body) use (&$attempts): void {
            $message = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
            $attempts[] = [$topic, $message['payload']['sequence']];

            if ($topic === 'orders') {
                throw PublicationException::notSent(new RuntimeException('Broker unavailable.'));
            }
        });

    Spoolrail::extend('selective', static fn (): Driver => $driver);

    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'known-failure']),
    );
    $this->artisan('spoolrail:outbox:publish')->run();
    $attempts = [];

    Spoolrail::connection('events')->publish(
        'returns',
        Message::make('return.created', ['sequence' => 'fresh']),
    );

    // --- Act ---
    $this->artisan('spoolrail:outbox:publish')->run();

    // --- Assert ---
    expect($attempts)->toBe([
        ['returns', 'fresh'],
        ['orders', 'known-failure'],
    ]);
});

test('leaves rows staged during dispatch for the next bounded run', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'staging']);

    $attemptedMessageIds = [];
    $stagedDuringDispatch = null;
    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->shouldReceive('publish')
        ->andReturnUsing(function (string $topic, string $body) use (&$attemptedMessageIds, &$stagedDuringDispatch): void {
            $message = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
            $attemptedMessageIds[] = $message['id'];

            if (! $stagedDuringDispatch instanceof Message) {
                $stagedDuringDispatch = Spoolrail::connection('events')->publish(
                    'orders',
                    Message::make('order.created', ['sequence' => 'later']),
                );
            }
        });

    Spoolrail::extend('staging', static fn (): Driver => $driver);

    $initial = Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'initial']),
    );

    // --- Act ---
    $firstExitCode = $this->artisan('spoolrail:outbox:publish')->run();

    // --- Assert ---
    expect($firstExitCode)->toBe(0);
    expect($attemptedMessageIds)->toBe([$initial->id]);
    expect(DB::table('outbox_publications')->count())->toBe(1);

    // --- Act ---
    $secondExitCode = $this->artisan('spoolrail:outbox:publish')->run();

    // --- Assert ---
    expect($secondExitCode)->toBe(0);
    expect($attemptedMessageIds)->toBe([$initial->id, $stagedDuringDispatch->id]);
    expect(DB::table('outbox_publications')->count())->toBe(0);
});

test('resolves the stored connection name from current configuration', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'retired']);

    Spoolrail::extend(
        'retired',
        static fn (): never => throw new RuntimeException('The retired connection was resolved.'),
    );

    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
    );

    config()->set('spoolrail.connections.events', ['driver' => 'current']);

    $published = 0;
    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->expects('publish')
        ->once()
        ->andReturnUsing(function () use (&$published): void {
            $published++;
        });

    Spoolrail::extend('current', static fn (): Driver => $driver);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:outbox:publish')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($published)->toBe(1);
    expect(DB::table('outbox_publications')->count())->toBe(0);
});

test('stores a single-line failure summary limited to five hundred characters', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'failing']);

    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->expects('publish')
        ->once()
        ->andThrow(new RuntimeException(str_repeat('x', 300)."\n".str_repeat('y', 300)));

    Spoolrail::extend('failing', static fn (): Driver => $driver);
    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
    );

    // --- Act ---
    $this->artisan('spoolrail:outbox:publish')->run();

    // --- Assert ---
    $summary = DB::table('outbox_publications')->sole()->last_error;

    expect(strlen((string) $summary))->toBe(500);
    expect($summary)->not->toContain("\n");
    expect($summary)->toStartWith(str_repeat('x', 300).' ');
});

test('reports every failed attempt when the reporting cache is unavailable', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'failing']);

    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->shouldReceive('publish')
        ->twice()
        ->andThrow(PublicationException::notSent(new RuntimeException('Broker unavailable.')));

    Spoolrail::extend('failing', static fn (): Driver => $driver);
    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
    );

    $limiter = Mockery::mock(RateLimiter::class);
    $limiter->shouldReceive('tooManyAttempts')
        ->twice()
        ->andThrow(new RuntimeException('Cache unavailable.'));
    app()->instance(RateLimiter::class, $limiter);

    $handler = Mockery::mock(ExceptionHandler::class);
    $handler->expects('report')->twice();
    app()->instance(ExceptionHandler::class, $handler);

    // --- Act ---
    $firstExitCode = $this->artisan('spoolrail:outbox:publish')->run();
    $secondExitCode = $this->artisan('spoolrail:outbox:publish')->run();

    // --- Assert ---
    expect($firstExitCode)->toBe(1);
    expect($secondExitCode)->toBe(1);
    expect(DB::table('outbox_publications')->count())->toBe(1);
});

test('throttles failure reports per row and logs recovery after deletion', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'recovering']);

    $attempt = 0;
    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->shouldReceive('publish')
        ->times(3)
        ->andReturnUsing(function () use (&$attempt): void {
            $attempt++;

            if ($attempt <= 2) {
                throw PublicationException::outcomeUnknown(
                    new RuntimeException("Confirmation timed out.\nAwaiting broker."),
                );
            }
        });

    Spoolrail::extend('recovering', static fn (): Driver => $driver);

    $message = Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
    );
    $rowId = DB::table('outbox_publications')->sole()->id;

    $reported = [];
    $handler = Mockery::mock(ExceptionHandler::class);
    $handler->shouldReceive('report')
        ->once()
        ->andReturnUsing(function (OutboxPublicationException $exception) use (&$reported): void {
            $reported[] = $exception;
        });
    app()->instance(ExceptionHandler::class, $handler);

    Log::shouldReceive('notice')
        ->once()
        ->withArgs(function (string $message, array $context) use ($rowId): bool {
            expect(DB::table('outbox_publications')->where('id', $rowId)->doesntExist())->toBeTrue();
            expect($message)->toBe('Spoolrail outbox publication recovered.');
            expect($context['outbox_id'])->toBe($rowId);

            return true;
        });

    // --- Act ---
    CarbonImmutable::setTestNow('2026-08-06 12:00:00 UTC');
    $firstExitCode = $this->artisan('spoolrail:outbox:publish')->run();
    $firstUpdatedAt = DB::table('outbox_publications')->sole()->updated_at;
    CarbonImmutable::setTestNow('2026-08-06 12:00:01 UTC');
    $secondExitCode = $this->artisan('spoolrail:outbox:publish')->run();
    $secondRow = DB::table('outbox_publications')->sole();
    CarbonImmutable::setTestNow('2026-08-06 12:00:02 UTC');
    $thirdExitCode = $this->artisan('spoolrail:outbox:publish')->run();

    // --- Assert ---
    expect($firstExitCode)->toBe(1);
    expect($secondExitCode)->toBe(1);
    expect($thirdExitCode)->toBe(0);
    expect($secondRow->last_error)
        ->toBe('The transport did not confirm the publication; the message may have been accepted.');
    expect($secondRow->updated_at)->not->toBe($firstUpdatedAt);
    expect($reported)->toHaveCount(1);
    expect($reported[0]->outboxId)->toBe($rowId);
    expect($reported[0]->logicalMessage)->toEqual($message);
    expect($reported[0]->connectionName)->toBe('events');
    expect($reported[0]->topic)->toBe('orders');
    expect($reported[0]->outcome)->toBe(PublicationOutcome::Unknown);
    expect($reported[0]->context())->toBe([
        'spoolrail_outbox_id' => $rowId,
        'spoolrail_message_id' => $message->id,
        'spoolrail_connection' => 'events',
        'spoolrail_topic' => 'orders',
        'spoolrail_publication_outcome' => 'Unknown',
    ]);
    expect($reported[0]->getPrevious())->toBeInstanceOf(PublicationException::class);
    expect(DB::table('outbox_publications')->count())->toBe(0);
});
