<?php

declare(strict_types=1);

use Illuminate\Database\QueryException;
use Illuminate\Log\Events\MessageLogged;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\ManuallyFailedException;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
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

test('queues one job when the same message is delivered again', function (): void {
    // --- Arrange ---
    $this->createJobsTable();

    Spoolrail::subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class)
        ->onQueueConnection('database');

    // Reusing the message preserves its UUID, as a broker redelivery does.
    $message = Message::make('order.created', []);
    Spoolrail::publish('orders', $message);
    Spoolrail::publish('orders', $message);

    // --- Act ---
    $this->artisan('spoolrail:consume warehouse-order-processing')->run();

    // --- Assert ---
    expect(DB::connection('testing')->table('jobs')->count())->toBe(1);
});

test('queues the same message once for each subscription', function (): void {
    // --- Arrange ---
    $this->createJobsTable();

    Spoolrail::subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    Spoolrail::subscribe('orders', 'billing-order-processing', RecordingMessageHandler::class)
        ->onQueueConnection('database');

    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume warehouse-order-processing')->run();
    $this->artisan('spoolrail:consume billing-order-processing')->run();

    // --- Assert ---
    expect(DB::connection('testing')->table('jobs')->count())->toBe(2);
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
    expect($failure)->toBeInstanceOf(LogicException::class);
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

test('invokes the failure callback once after Laravel exhausts asynchronous attempts without redelivering the source', function (): void {
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
        $this->artisan('queue:work database --once --sleep=0')->run();
    }

    $this->artisan('spoolrail:consume worker-failure')->run();

    // --- Assert ---
    expect($failedJobs)->toHaveCount(1);
    expect(RecordingMessageHandler::$attempts)->toBe(4);
    expect(RecordingMessageHandler::$failedMessages)->toHaveCount(1);
    expect(RecordingMessageHandler::$failedMessages[0]->id)
        ->toBe(RecordingMessageHandler::$attemptedMessages[0]->id);
    expect(RecordingMessageHandler::$failureCauses[0]?->getMessage())->toBe('Handler failed.');
    expect(array_map(
        static fn (Message $message): mixed => $message->transport,
        RecordingMessageHandler::$attemptedMessages,
    ))->each->toEqual(RecordingMessageHandler::$attemptedMessages[0]->transport);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

test('redelivers a sync delivery and invokes the failure callback for each failed Queue job', function (): void {
    // --- Arrange ---
    RecordingMessageHandler::$handlerFailuresRemaining = 2;

    Spoolrail::subscribe('orders', 'sync-failure', RecordingMessageHandler::class);
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $failures = [];

    foreach (range(1, 2) as $_) {
        try {
            $this->artisan('spoolrail:consume sync-failure')->run();
        } catch (Throwable $exception) {
            $failures[] = $exception;
        }
    }

    $this->artisan('spoolrail:consume sync-failure')->run();
    $this->artisan('spoolrail:consume sync-failure')->run();

    // --- Assert ---
    expect($failures)->toHaveCount(2);
    expect($failures[0]->getMessage())->toBe('Handler failed.');
    expect($failures[1]->getMessage())->toBe('Handler failed.');
    expect(RecordingMessageHandler::$attempts)->toBe(3);
    expect(RecordingMessageHandler::$failedMessages)->toHaveCount(2);
    expect(RecordingMessageHandler::$failedMessages[0]->id)->toBe($published->id);
    expect(RecordingMessageHandler::$failedMessages[1]->id)->toBe($published->id);
    expect(RecordingMessageHandler::$messages[0]->id)->toBe($published->id);
    expect(RecordingMessageHandler::$attemptedMessages[0]->id)->toBe($published->id);
    expect(RecordingMessageHandler::$attemptedMessages[1]->id)->toBe($published->id);
    expect(RecordingMessageHandler::$attemptedMessages[0]->transport)
        ->not->toBe(RecordingMessageHandler::$attemptedMessages[1]->transport);
    expect(RecordingMessageHandler::$attemptedMessages[0]->transport?->redelivered)->toBeFalse();
    expect(RecordingMessageHandler::$attemptedMessages[1]->transport?->redelivered)->toBeTrue();
});

test('passes a null cause to the handler when middleware fails a job without an exception', function (): void {
    // --- Arrange ---
    $this->createJobsTable();
    config()->set('queue.failed.driver', 'null');

    $failedJobs = [];
    Event::listen(JobFailed::class, function (JobFailed $event) use (&$failedJobs): void {
        $failedJobs[] = $event;
    });

    RecordingMessageHandler::$failWithoutException = true;

    Spoolrail::subscribe('orders', 'manual-failure', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume manual-failure')->run();
    $this->artisan('queue:work database --once --sleep=0')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$attempts)->toBe(0);
    expect(RecordingMessageHandler::$failedMessages)->toHaveCount(1);
    expect(RecordingMessageHandler::$failedMessages[0]->id)->toBe($published->id);
    expect(RecordingMessageHandler::$failureCauses)->toBe([null]);
    expect($failedJobs)->toHaveCount(1);
    expect($failedJobs[0]->exception)->toBeInstanceOf(ManuallyFailedException::class);
});

test('preserves the original Laravel failure when the queued subscription no longer resolves', function (): void {
    // --- Arrange ---
    $this->createJobsTable();
    config()->set('queue.failed.driver', 'null');

    $failedJobs = [];
    Event::listen(JobFailed::class, function (JobFailed $event) use (&$failedJobs): void {
        $failedJobs[] = $event;
    });

    Spoolrail::subscribe('orders', 'removed-subscription', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', []));
    $this->artisan('spoolrail:consume removed-subscription')->run();

    app()->instance(SubscriptionRegistry::class, new SubscriptionRegistry);

    // --- Act ---
    foreach (range(1, 4) as $_) {
        $this->artisan('queue:work database --once --sleep=0')->run();
    }

    // --- Assert ---
    expect($failedJobs)->toHaveCount(1);
    expect($failedJobs[0]->exception)->toBeInstanceOf(InvalidSubscriptionException::class);
    expect(RecordingMessageHandler::$failedMessages)->toBe([]);
});

test('propagates failure callback exceptions after Laravel reports the original failure', function (): void {
    // --- Arrange ---
    $failedJobs = [];
    Event::listen(JobFailed::class, function (JobFailed $event) use (&$failedJobs): void {
        $failedJobs[] = $event;
    });

    RecordingMessageHandler::$handlerFailuresRemaining = 1;
    RecordingMessageHandler::$callbackFailure = new RuntimeException('Failure callback failed.');

    Spoolrail::subscribe('orders', 'callback-failure', RecordingMessageHandler::class);
    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume callback-failure')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    RecordingMessageHandler::$callbackFailure = null;
    $this->artisan('spoolrail:consume callback-failure')->run();

    // --- Assert ---
    expect($failure?->getMessage())->toBe('Failure callback failed.');
    expect($failedJobs)->toHaveCount(1);
    expect($failedJobs[0]->exception->getMessage())->toBe('Handler failed.');
    expect(RecordingMessageHandler::$failedMessages)->toHaveCount(1);
    expect(RecordingMessageHandler::$messages)->toHaveCount(1);
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
