<?php

declare(strict_types=1);

use Illuminate\Database\QueryException;
use Illuminate\Log\Events\MessageLogged;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Spoolrail\Spoolrail\Exceptions\DatabaseQueueTransactionException;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithDatabaseQueue;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithDatabaseQueue::class);

beforeEach(function (): void {
    RecordingMessageHandler::reset();
});

test('rejects a queued-message drain name as an active subscription', function (): void {
    Spoolrail::subscribe('orders', 'warehouse-order-processing-v2', RecordingMessageHandler::class)
        ->drainMessagesQueuedFor('warehouse-order-processing');

    expect(fn () => $this->artisan('spoolrail:consume warehouse-order-processing')->run())
        ->toThrow(InvalidSubscriptionException::class);
});

test('rejects an unknown subscription', function (): void {
    expect(fn () => $this->artisan('spoolrail:consume missing-subscription')->run())
        ->toThrow(
            InvalidSubscriptionException::class,
            'Subscription [missing-subscription] has not been registered.',
        );
});

test('uses the subscription Laravel Queue connection and queue overrides', function (): void {
    // --- Arrange ---
    $this->createJobsTable();

    Spoolrail::subscribe('orders', 'priority-orders', RecordingMessageHandler::class)
        ->onQueueConnection('database')
        ->onQueue('courier-broker');
    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume priority-orders')->run();

    // --- Assert ---
    expect(DB::connection('testing')->table('jobs')->pluck('queue')->all())
        ->toBe(['courier-broker']);
});

test('rejects an open transaction on the database Queue connection without losing the delivery', function (string $transactionApi): void {
    // --- Arrange ---
    $this->createJobsTable();

    Spoolrail::subscribe('orders', 'transaction-orders', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', []));

    $database = DB::connection('testing');
    $pdo = $database->getPdo();

    if ($transactionApi === 'laravel') {
        $database->beginTransaction();
    } else {
        $pdo->beginTransaction();
    }

    // --- Act ---
    try {
        $failure = null;

        try {
            $this->artisan('spoolrail:consume transaction-orders')->run();
        } catch (Throwable $exception) {
            $failure = $exception;
        }

        $queuedBeforeRollback = $database->table('jobs')->count();
    } finally {
        if ($transactionApi === 'laravel') {
            $database->rollBack();
        } else {
            $pdo->rollBack();
        }
    }

    $this->artisan('spoolrail:consume transaction-orders')->run();

    // --- Assert ---
    expect($failure)->toBeInstanceOf(DatabaseQueueTransactionException::class);
    expect($failure?->getMessage())->toBe(
        "Laravel's database Queue cannot accept a Spoolrail handoff while its connection has an open transaction. Commit or roll back that transaction before consuming, or use another Queue connection.",
    );
    expect($queuedBeforeRollback)->toBe(0);
    expect($database->table('jobs')->pluck('queue')->all())->toBe(['default']);
})->with([
    'Laravel connection API' => 'laravel',
    'direct PDO API' => 'pdo',
]);

test('uses a database Queue while an unrelated database connection has an open transaction', function (): void {
    // --- Arrange ---
    $this->createJobsTable();

    config()->set('database.connections.unrelated', [
        'driver' => 'sqlite',
        'database' => ':memory:',
        'prefix' => '',
    ]);
    Spoolrail::subscribe('orders', 'independent-transaction-orders', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', []));

    $unrelated = DB::connection('unrelated');
    $unrelated->beginTransaction();

    // --- Act ---
    try {
        $this->artisan('spoolrail:consume independent-transaction-orders')->run();
    } finally {
        $unrelated->rollBack();
    }

    // --- Assert ---
    expect(DB::connection('testing')->table('jobs')->pluck('queue')->all())
        ->toBe(['default']);
});

test('leaves queued handler failures to Laravel Queue without redelivering the source delivery', function (): void {
    // --- Arrange ---
    $this->createJobsTable();
    config()->set('queue.failed.driver', 'null');

    $failedJobs = [];
    Event::listen(JobFailed::class, function (JobFailed $event) use (&$failedJobs): void {
        $failedJobs[] = $event;
    });

    RecordingMessageHandler::$handlerFailuresRemaining = 4;

    Spoolrail::subscribe('orders', 'worker-failure', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume worker-failure')->run();

    foreach (range(1, 4) as $_) {
        $this->artisan('queue:work database --once')->run();
    }

    $this->artisan('spoolrail:consume worker-failure')->run();

    // --- Assert ---
    expect($failedJobs)->toHaveCount(1);
    expect(RecordingMessageHandler::$attempts)->toBe(4);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

test('redelivers a sync delivery after its handler fails during handoff', function (): void {
    // --- Arrange ---
    RecordingMessageHandler::$handlerFailuresRemaining = 1;

    Spoolrail::subscribe('orders', 'sync-failure', RecordingMessageHandler::class);
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume sync-failure')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $this->artisan('spoolrail:consume sync-failure')->run();
    $this->artisan('spoolrail:consume sync-failure')->run();

    // --- Assert ---
    expect($failure?->getMessage())->toBe('Handler failed.');
    expect(RecordingMessageHandler::$attempts)->toBe(2);
    expect(RecordingMessageHandler::$messages)->toEqual([$published]);
});

test('propagates a rejected database Queue handoff without logging or losing buffered deliveries', function (): void {
    // --- Arrange ---
    $logs = [];
    Event::listen(MessageLogged::class, function (MessageLogged $event) use (&$logs): void {
        $logs[] = $event;
    });

    Spoolrail::subscribe('orders', 'queue-failure', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', ['sequence' => 1]));
    Spoolrail::publish('orders', Message::make('order.created', ['sequence' => 2]));

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume queue-failure')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $this->createJobsTable();
    $this->artisan('spoolrail:consume queue-failure')->run();
    $this->artisan('queue:work database --once')->run();
    $this->artisan('queue:work database --once')->run();

    // --- Assert ---
    expect($failure)->toBeInstanceOf(QueryException::class);
    expect(array_map(
        static fn (Message $message): int => $message->payload['sequence'],
        RecordingMessageHandler::$messages,
    ))->toBe([1, 2]);
    expect($logs)->toBe([]);
});
