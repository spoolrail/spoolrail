<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Log\Events\MessageLogged;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Schema;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Exceptions\DatabaseQueueTransactionException;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageSerializer;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

test('consumes from the subscription configured Spoolrail connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.secondary', ['driver' => 'array']);
    $handled = [];
    $handler = Mockery::mock(NoopMessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function (Message $message) use (&$handled): void {
            $handled[] = $message;
        });
    app()->instance(NoopMessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'secondary-orders', NoopMessageHandler::class)
        ->onConnection('secondary');

    Spoolrail::connection('secondary')->publish(
        'orders',
        Message::make('activity.recorded', ['reference' => 'secondary']),
    );

    // --- Act ---
    $this->artisan('spoolrail:consume secondary-orders')->run();

    // --- Assert ---
    expect($handled)->toHaveCount(1);
    expect($handled[0]->payload['reference'])->toBe('secondary');
});

test('rejects a queued-message drain name as an active subscription', function (): void {
    Spoolrail::subscribe('orders', 'warehouse-order-processing-v2', NoopMessageHandler::class)
        ->drainMessagesQueuedFor('warehouse-order-processing');

    expect(fn () => $this->artisan('spoolrail:consume warehouse-order-processing')->run())
        ->toThrow(InvalidSubscriptionException::class);
});

test('gives each matching subscription one independently hydrated delivery', function (): void {
    // --- Arrange ---
    $handled = [];
    $handler = Mockery::mock(NoopMessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function (Message $message) use (&$handled): void {
            $handled[] = $message;
        });
    app()->instance(NoopMessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'first-orders', NoopMessageHandler::class);
    Spoolrail::subscribe('orders', 'second-orders', NoopMessageHandler::class);

    $published = Spoolrail::publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
    );

    // --- Act ---
    $this->artisan('spoolrail:consume first-orders')->run();
    $this->artisan('spoolrail:consume second-orders')->run();

    // --- Assert ---
    expect($handled)->toEqual([$published, $published]);
    expect($handled[0])->not->toBe($published);
    expect($handled[1])->not->toBe($published);
    expect($handled[1])->not->toBe($handled[0]);
});

test('uses the subscription Laravel Queue connection and queue overrides', function (): void {
    // --- Arrange ---
    createConsumeSubscriptionJobsTable();

    Spoolrail::subscribe('orders', 'priority-orders', NoopMessageHandler::class)
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
    createConsumeSubscriptionJobsTable();

    Spoolrail::subscribe('orders', 'transaction-orders', NoopMessageHandler::class)
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
    createConsumeSubscriptionJobsTable();

    config()->set('database.connections.unrelated', [
        'driver' => 'sqlite',
        'database' => ':memory:',
        'prefix' => '',
    ]);
    Spoolrail::subscribe('orders', 'independent-transaction-orders', NoopMessageHandler::class)
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
    createConsumeSubscriptionJobsTable();
    Event::fake([JobFailed::class]);

    config()->set('queue.failed.driver', 'null');

    $attempts = 0;
    $handler = Mockery::mock(NoopMessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function () use (&$attempts): void {
            $attempts++;

            throw new RuntimeException('Worker handler failed.');
        });
    app()->instance(NoopMessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'worker-failure', NoopMessageHandler::class)
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume worker-failure')->run();
    $this->artisan('queue:work database --once --tries=1')->run();
    $this->artisan('spoolrail:consume worker-failure')->run();

    // --- Assert ---
    Event::assertDispatched(JobFailed::class);
    expect($attempts)->toBe(1);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

test('redelivers a sync delivery after its handler fails during handoff', function (): void {
    // --- Arrange ---
    $shouldFail = true;
    $attempts = 0;
    $handled = 0;

    $handler = Mockery::mock(NoopMessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function () use (&$shouldFail, &$attempts, &$handled): void {
            $attempts++;

            if ($shouldFail) {
                throw new RuntimeException('Sync handler failed.');
            }

            $handled++;
        });
    app()->instance(NoopMessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'sync-failure', NoopMessageHandler::class);
    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume sync-failure')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $shouldFail = false;
    $this->artisan('spoolrail:consume sync-failure')->run();
    $this->artisan('spoolrail:consume sync-failure')->run();

    // --- Assert ---
    expect($failure)->toBeInstanceOf(RuntimeException::class);
    expect($failure?->getMessage())->toBe('Sync handler failed.');
    expect($attempts)->toBe(2);
    expect($handled)->toBe(1);
});

test('propagates a rejected Queue handoff without logging or losing buffered deliveries', function (): void {
    // --- Arrange ---
    Event::fake([MessageLogged::class]);

    $handled = [];
    $handler = Mockery::mock(NoopMessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function (Message $message) use (&$handled): void {
            $handled[] = $message;
        });
    app()->instance(NoopMessageHandler::class, $handler);

    $syncQueues = app(QueueFactory::class);

    Spoolrail::subscribe('orders', 'queue-failure', NoopMessageHandler::class);
    Spoolrail::publish('orders', Message::make('order.created', ['sequence' => 1]));
    Spoolrail::publish('orders', Message::make('order.created', ['sequence' => 2]));

    $handoffFailure = new RuntimeException('Queue is unavailable.');
    $queue = Mockery::mock(QueueContract::class);
    $queue->shouldReceive('push')
        ->once()
        ->andThrow($handoffFailure);

    $failingQueues = Mockery::mock(QueueFactory::class);
    $failingQueues->shouldReceive('connection')
        ->once()
        ->andReturn($queue);
    app()->instance(QueueFactory::class, $failingQueues);

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume queue-failure')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    app()->instance(QueueFactory::class, $syncQueues);
    $this->artisan('spoolrail:consume queue-failure')->run();

    // --- Assert ---
    $sequences = array_map(
        fn (Message $message): int => $message->payload['sequence'],
        $handled,
    );

    expect($failure)->toBe($handoffFailure);
    expect($sequences)->toBe([1, 2]);
    Event::assertNotDispatched(MessageLogged::class);
});

test('redelivers malformed JSON and stops the current delivery drain', function (): void {
    // --- Arrange ---
    $handler = Mockery::mock(NoopMessageHandler::class);
    $handler->allows('handle');
    app()->instance(NoopMessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'malformed-orders', NoopMessageHandler::class);

    $driver = new ArrayDriver(
        'array',
        'array',
        app(SubscriptionRegistry::class),
    );
    $driver->publish('orders', '');
    $driver->publish(
        'orders',
        (new MessageSerializer)->serialize(
            Message::make('order.created', ['sequence' => 2])
                ->withPublishedAt(CarbonImmutable::parse('2026-07-15 14:23:08.417 UTC')),
        ),
    );

    Spoolrail::extend('array', fn (): ArrayDriver => $driver);

    // --- Act ---
    $consume = fn () => $this->artisan('spoolrail:consume malformed-orders')->run();

    // --- Assert ---
    expect($consume)->toThrow(JsonException::class);
    expect($consume)->toThrow(JsonException::class);
    $handler->shouldNotHaveReceived('handle');
});

test('rejects an unknown subscription', function (): void {
    expect(fn () => $this->artisan('spoolrail:consume missing-subscription')->run())
        ->toThrow(
            InvalidSubscriptionException::class,
            'Subscription [missing-subscription] has not been registered.',
        );
});

function createConsumeSubscriptionJobsTable(): void
{
    Schema::connection('testing')->create('jobs', function (Blueprint $blueprint): void {
        $blueprint->id();
        $blueprint->string('queue')->index();
        $blueprint->longText('payload');
        $blueprint->unsignedTinyInteger('attempts');
        $blueprint->unsignedInteger('reserved_at')->nullable();
        $blueprint->unsignedInteger('available_at');
        $blueprint->unsignedInteger('created_at');
    });
}
