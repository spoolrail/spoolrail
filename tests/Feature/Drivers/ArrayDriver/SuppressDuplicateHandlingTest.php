<?php

declare(strict_types=1);

use Illuminate\Contracts\Cache\Repository;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Queue\Middleware\WithoutOverlapping;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithDatabaseQueue;
use Spoolrail\Spoolrail\Tests\Fixtures\LongRunningMessageHandler;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithDatabaseQueue::class);

beforeEach(function (): void {
    RecordingMessageHandler::reset();
});

test('handles a redelivered message only once', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);

    // Publishing one message twice buffers two deliveries that share a
    // message id, exactly like a broker redelivering an unacknowledged one.
    $message = Message::make('order.created', []);
    Spoolrail::publish('orders', $message);
    Spoolrail::publish('orders', $message);

    // --- Act ---
    $this->artisan('spoolrail:consume warehouse-order-processing')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$attempts)->toBe(1);
    expect(array_map(
        static fn (Message $handled): string => $handled->id,
        RecordingMessageHandler::$messages,
    ))->toBe([$message->id]);
});

test('handles a message once per subscription', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'billing-order-processing', RecordingMessageHandler::class);

    $message = Message::make('order.created', []);
    Spoolrail::publish('orders', $message);

    // --- Act ---
    $this->artisan('spoolrail:consume warehouse-order-processing')->run();
    $this->artisan('spoolrail:consume billing-order-processing')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$attempts)->toBe(2);
});

test('handles every delivery when deduplication is disabled', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.deduplication.enabled', false);

    Spoolrail::subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);

    $message = Message::make('order.created', []);
    Spoolrail::publish('orders', $message);
    Spoolrail::publish('orders', $message);

    // --- Act ---
    $this->artisan('spoolrail:consume warehouse-order-processing')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$attempts)->toBe(2);
});

test('refuses to consume with a lockless deduplication store before any handoff', function (): void {
    // --- Arrange ---
    $this->createJobsTable();
    $locklessStore = Mockery::mock(Store::class);
    Cache::extend('lockless', fn (): Repository => Cache::repository($locklessStore));
    config()->set('cache.stores.lockless', ['driver' => 'lockless']);
    config()->set('spoolrail.deduplication.store', 'lockless');

    Spoolrail::subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume warehouse-order-processing')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // An empty jobs table proves the refusal happened at startup, before
    // the source delivery was handed off, not in the worker-side guard.
    $queuedWhileRefused = DB::connection('testing')->table('jobs')->count();

    config()->set('spoolrail.deduplication.store', null);
    $this->artisan('spoolrail:consume warehouse-order-processing')->run();
    $this->artisan('queue:work database --once --sleep=0')->run();

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidConfigException::class);
    expect($failure?->getMessage())->toBe(
        'Spoolrail deduplication requires a cache store that supports atomic locks, and the [lockless] cache store does not. Configure [spoolrail.deduplication.store] with a lock-capable store or disable [spoolrail.deduplication.enabled].',
    );
    expect($queuedWhileRefused)->toBe(0);
    expect(RecordingMessageHandler::$attempts)->toBe(1);
});

test('holds the handling lock for the handler timeout plus a shutdown margin', function (): void {
    // --- Arrange ---
    $this->createJobsTable();
    config()->set('queue.connections.database.retry_after', 600);
    config()->set('spoolrail.deduplication.store', 'array');
    config()->set('spoolrail.deduplication.lock', 30);

    // The handler declares a 120-second timeout, so one attempt may hold
    // the lock for 180 seconds: past the 30-second configured floor and
    // past the timeout itself, leaving a shutdown margin.
    Spoolrail::subscribe('orders', 'warehouse-order-processing', LongRunningMessageHandler::class)
        ->onQueueConnection('database');

    $message = Message::make('order.created', []);
    Spoolrail::publish('orders', $message);
    Spoolrail::publish('orders', $message);
    $this->artisan('spoolrail:consume warehouse-order-processing')->run();

    $attemptsBeyondConfiguredLock = null;
    $attemptsBeyondTimeout = null;

    LongRunningMessageHandler::$duringHandling = function () use (
        &$attemptsBeyondConfiguredLock,
        &$attemptsBeyondTimeout,
    ): void {
        $this->travel(40)->seconds();
        $this->artisan('queue:work database --once --sleep=0')->run();
        $attemptsBeyondConfiguredLock = RecordingMessageHandler::$attempts;

        $this->travel(110)->seconds();
        $this->artisan('queue:work database --once --sleep=0')->run();
        $attemptsBeyondTimeout = RecordingMessageHandler::$attempts;
    };

    // --- Act ---
    $this->artisan('queue:work database --once --sleep=0')->run();

    $this->travel(11)->seconds();
    $this->artisan('queue:work database --once --sleep=0')->run();

    // --- Assert ---
    expect($attemptsBeyondConfiguredLock)->toBe(1);
    expect($attemptsBeyondTimeout)->toBe(1);
    expect(RecordingMessageHandler::$attempts)->toBe(1);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

test('does not remember an attempt that handler middleware releases', function (): void {
    // --- Arrange ---
    $this->createJobsTable();

    Spoolrail::subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class)
        ->onQueueConnection('database');

    $message = Message::make('order.created', []);
    Spoolrail::publish('orders', $message);
    $this->artisan('spoolrail:consume warehouse-order-processing')->run();

    // Hold the handler middleware's lock so its first attempt is released
    // after duplicate suppression has admitted it.
    $handlerMiddleware = new WithoutOverlapping($message->id);
    $handlerLock = Cache::lock(
        $handlerMiddleware->getLockKey(
            new HandleMessageJob($message, 'warehouse-order-processing'),
        ),
        60,
    );
    expect($handlerLock->get())->toBeTrue();

    // --- Act ---
    $this->artisan('queue:work database --once --sleep=0')->run();
    $releasedAttempts = RecordingMessageHandler::$attempts;

    $handlerLock->release();
    $this->artisan('queue:work database --once --sleep=0')->run();

    // --- Assert ---
    expect($releasedAttempts)->toBe(0);
    expect(RecordingMessageHandler::$attempts)->toBe(1);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

test('releases a duplicate back to the queue while the message is still being handled', function (): void {
    // --- Arrange ---
    $this->createJobsTable();

    Spoolrail::subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class)
        ->onQueueConnection('database');

    $message = Message::make('order.created', []);
    Spoolrail::publish('orders', $message);
    Spoolrail::publish('orders', $message);
    $this->artisan('spoolrail:consume warehouse-order-processing')->run();

    // Hold the handling lock as a concurrent worker running the other
    // duplicate would. Skipping now could lose the message if that
    // worker crashed, so both jobs must survive until the lock clears.
    $lock = Cache::lock(
        'spoolrail:handled:'.hash('xxh128', "warehouse-order-processing:$message->id").':lock',
        60,
    );
    expect($lock->get())->toBeTrue();

    // --- Act ---
    $this->artisan('queue:work database --once --sleep=0')->run();
    $this->artisan('queue:work database --once --sleep=0')->run();

    $queuedWhileLocked = DB::connection('testing')->table('jobs')->count();
    $attemptsWhileLocked = RecordingMessageHandler::$attempts;

    $lock->release();
    $this->travel(11)->seconds();

    $this->artisan('queue:work database --once --sleep=0')->run();
    $this->artisan('queue:work database --once --sleep=0')->run();

    // --- Assert ---
    expect($attemptsWhileLocked)->toBe(0);
    expect($queuedWhileLocked)->toBe(2);
    expect(RecordingMessageHandler::$attempts)->toBe(1);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(0);
});
