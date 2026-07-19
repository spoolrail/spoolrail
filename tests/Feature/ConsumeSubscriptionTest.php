<?php

declare(strict_types=1);

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Log\Events\MessageLogged;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Schema;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Exceptions\ConnectionNotConsumableException;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

test('routes each subscription through its topic and configured Spoolrail connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.secondary', ['driver' => 'array']);
    config()->set('queue.default', 'sync');

    $handled = [];
    $handler = Mockery::mock(MessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function (Message $message) use (&$handled): void {
            $handled[] = $message;
        });
    app()->instance(MessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'warehouse-orders', MessageHandler::class);
    Spoolrail::subscribe('returns', 'warehouse-returns', MessageHandler::class);
    Spoolrail::subscribe('orders', 'secondary-orders', MessageHandler::class)
        ->onConnection('secondary');

    Spoolrail::publish(
        'orders',
        Message::make('activity.recorded', ['reference' => 'A-42']),
    );
    Spoolrail::publish(
        'returns',
        Message::make('activity.recorded', ['reference' => 'R-21']),
    );
    Spoolrail::connection('secondary')->publish(
        'orders',
        Message::make('activity.recorded', ['reference' => 'B-84']),
    );

    // --- Act ---
    $this->artisan('spoolrail:consume warehouse-orders')->run();
    $afterOrders = consumeSubscriptionReferences($handled);

    $this->artisan('spoolrail:consume warehouse-returns')->run();
    $afterReturns = consumeSubscriptionReferences($handled);

    $this->artisan('spoolrail:consume secondary-orders')->run();
    $afterSecondary = consumeSubscriptionReferences($handled);

    // --- Assert ---
    expect($afterOrders)->toBe(['A-42']);
    expect($afterReturns)->toBe(['A-42', 'R-21']);
    expect($afterSecondary)->toBe(['A-42', 'R-21', 'B-84']);
});

test('gives each matching subscription one independently hydrated delivery', function (): void {
    // --- Arrange ---
    config()->set('queue.default', 'sync');

    $handled = [];
    $handler = Mockery::mock(MessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function (Message $message) use (&$handled): void {
            $handled[] = $message;
        });
    app()->instance(MessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'first-orders', MessageHandler::class);
    Spoolrail::subscribe('orders', 'second-orders', MessageHandler::class);

    $published = Spoolrail::publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
    );

    // --- Act ---
    $this->artisan('spoolrail:consume first-orders')->run();
    $afterFirstCount = count($handled);

    $this->artisan('spoolrail:consume second-orders')->run();
    $afterSecondCount = count($handled);

    // --- Assert ---
    expect($afterFirstCount)->toBe(1);
    expect($afterSecondCount)->toBe(2);
    expect($handled)->toEqual([$published, $published]);
    expect($handled[0])->not->toBe($published);
    expect($handled[1])->not->toBe($published);
    expect($handled[1])->not->toBe($handled[0]);
});

test('uses the application Laravel Queue destination when subscription overrides are absent', function (): void {
    // --- Arrange ---
    createConsumeSubscriptionJobsTable('application_jobs');
    createConsumeSubscriptionJobsTable('conventional_jobs');

    config()->set('queue.default', 'application-database');
    config()->set('queue.connections.application-database', [
        'driver' => 'database',
        'connection' => 'testing',
        'table' => 'application_jobs',
        'queue' => 'application-default',
        'retry_after' => 90,
        'after_commit' => false,
    ]);
    config()->set('queue.connections.database', [
        'driver' => 'database',
        'connection' => 'testing',
        'table' => 'conventional_jobs',
        'queue' => 'wrong-default',
        'retry_after' => 90,
        'after_commit' => false,
    ]);

    Spoolrail::subscribe('orders', 'default-orders', NoopMessageHandler::class);
    Spoolrail::publish('orders', Message::make('order.created', []));

    $database = DB::connection('testing');

    // --- Act ---
    $this->artisan('spoolrail:consume default-orders')->run();

    // --- Assert ---
    expect($database->table('application_jobs')->pluck('queue')->all())->toBe(['application-default']);
    expect($database->table('conventional_jobs')->count())->toBe(0);
});

test('uses the subscription Laravel Queue connection and queue overrides', function (): void {
    // --- Arrange ---
    createConsumeSubscriptionJobsTable('priority_jobs');
    createConsumeSubscriptionJobsTable('conventional_jobs');

    config()->set('queue.default', 'database');
    config()->set('queue.connections.priority-database', [
        'driver' => 'database',
        'connection' => 'testing',
        'table' => 'priority_jobs',
        'queue' => 'unused-default',
        'retry_after' => 90,
        'after_commit' => false,
    ]);
    config()->set('queue.connections.database', [
        'driver' => 'database',
        'connection' => 'testing',
        'table' => 'conventional_jobs',
        'queue' => 'wrong-default',
        'retry_after' => 90,
        'after_commit' => false,
    ]);

    Spoolrail::subscribe('orders', 'priority-orders', NoopMessageHandler::class)
        ->onQueueConnection('priority-database')
        ->onQueue('courier-broker');
    Spoolrail::publish('orders', Message::make('order.created', []));

    $database = DB::connection('testing');

    // --- Act ---
    $this->artisan('spoolrail:consume priority-orders')->run();

    // --- Assert ---
    expect($database->table('priority_jobs')->pluck('queue')->all())->toBe(['courier-broker']);
    expect($database->table('conventional_jobs')->count())->toBe(0);
});

test('pushes a delivery before an open database transaction commits', function (): void {
    // --- Arrange ---
    createConsumeSubscriptionJobsTable('transaction_jobs');

    config()->set('queue.default', 'transaction-database');
    config()->set('queue.connections.transaction-database', [
        'driver' => 'database',
        'connection' => 'testing',
        'table' => 'transaction_jobs',
        'queue' => 'transaction-default',
        'retry_after' => 90,
        'after_commit' => true,
    ]);

    Spoolrail::subscribe('orders', 'transaction-orders', NoopMessageHandler::class);
    Spoolrail::publish('orders', Message::make('order.created', []));

    $database = DB::connection('testing');
    $database->beginTransaction();

    try {
        // --- Act ---
        $this->artisan('spoolrail:consume transaction-orders')->run();

        // --- Assert ---
        expect($database->table('transaction_jobs')->count())->toBe(1);
    } finally {
        $database->rollBack();
    }
});

test('leaves queued handler failures to Laravel Queue without redelivering the source delivery', function (): void {
    // --- Arrange ---
    createConsumeSubscriptionJobsTable();
    Event::fake([JobFailed::class]);

    config()->set('queue.connections.database', [
        'driver' => 'database',
        'connection' => 'testing',
        'table' => 'jobs',
        'queue' => 'default',
        'retry_after' => 90,
        'after_commit' => false,
    ]);
    config()->set('queue.failed.driver', 'null');

    $attempts = 0;
    $handler = Mockery::mock(MessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function () use (&$attempts): void {
            $attempts++;

            throw new RuntimeException('Worker handler failed.');
        });
    app()->instance(MessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'worker-failure', MessageHandler::class)
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
    config()->set('queue.default', 'sync');

    $shouldFail = true;
    $attempts = 0;
    $handled = 0;

    $handler = Mockery::mock(MessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function () use (&$shouldFail, &$attempts, &$handled): void {
            $attempts++;

            if ($shouldFail) {
                throw new RuntimeException('Sync handler failed.');
            }

            $handled++;
        });
    app()->instance(MessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'sync-failure', MessageHandler::class);
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
    config()->set('queue.default', 'sync');
    Event::fake([MessageLogged::class]);

    $handled = [];
    $handler = Mockery::mock(MessageHandler::class);
    $handler->shouldReceive('handle')
        ->andReturnUsing(function (Message $message) use (&$handled): void {
            $handled[] = $message;
        });
    app()->instance(MessageHandler::class, $handler);

    $syncQueues = app(QueueFactory::class);

    Spoolrail::subscribe('orders', 'queue-failure', MessageHandler::class);
    Spoolrail::publish('orders', Message::make('order.created', ['sequence' => 1]));
    Spoolrail::publish('orders', Message::make('order.created', ['sequence' => 2]));

    $queue = Mockery::mock(QueueContract::class);
    $queue->shouldReceive('push')
        ->once()
        ->andThrow(new RuntimeException('Queue is unavailable.'));

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

    expect($failure)->toBeInstanceOf(RuntimeException::class);
    expect($failure?->getMessage())->toBe('Queue is unavailable.');
    expect($sequences)->toBe([1, 2]);
    Event::assertNotDispatched(MessageLogged::class);
});

test('leaves malformed JSON unacknowledged and stops the current delivery drain', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.malformed', ['driver' => 'malformed']);

    $handler = Mockery::mock(MessageHandler::class);
    $handler->allows('handle');
    app()->instance(MessageHandler::class, $handler);

    Spoolrail::subscribe('orders', 'malformed-orders', MessageHandler::class)
        ->onConnection('malformed');

    $driver = new ArrayDriver(
        'malformed',
        'array',
        app(SubscriptionRegistry::class),
    );
    $driver->publish('orders', '{"id":');
    $driver->publish('orders', consumeSubscriptionEnvelope(['sequence' => 2]));

    Spoolrail::extend('malformed', fn (): ArrayDriver => $driver);

    // --- Act ---
    $firstFailure = null;
    $secondFailure = null;

    try {
        $this->artisan('spoolrail:consume malformed-orders')->run();
    } catch (Throwable $exception) {
        $firstFailure = $exception;
    }

    try {
        $this->artisan('spoolrail:consume malformed-orders')->run();
    } catch (Throwable $exception) {
        $secondFailure = $exception;
    }

    // --- Assert ---
    expect($firstFailure)->toBeInstanceOf(JsonException::class);
    expect($secondFailure)->toBeInstanceOf(JsonException::class);
    $handler->shouldNotHaveReceived('handle');
});

test('rejects an unknown subscription', function (): void {
    // --- Arrange ---
    $subscription = 'missing-subscription';

    // --- Act ---
    $failure = null;

    try {
        $this->artisan("spoolrail:consume $subscription")->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidSubscriptionException::class);
    expect($failure?->getMessage())->toBe('Subscription [missing-subscription] has not been registered.');
});

test('rejects a subscription whose broker connection cannot consume', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.publish-only', ['driver' => 'publish-only']);

    $driver = Mockery::mock(Driver::class);
    Spoolrail::extend('publish-only', fn (): Driver => $driver);

    Spoolrail::subscribe('orders', 'publish-only-orders', NoopMessageHandler::class)
        ->onConnection('publish-only');

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume publish-only-orders')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(ConnectionNotConsumableException::class);
    expect($failure?->getMessage())->toBe('Spoolrail connection [publish-only] does not support consumption.');
});

function createConsumeSubscriptionJobsTable(string $table = 'jobs'): void
{
    Schema::connection('testing')->create($table, function (Blueprint $blueprint): void {
        $blueprint->id();
        $blueprint->string('queue')->index();
        $blueprint->longText('payload');
        $blueprint->unsignedTinyInteger('attempts');
        $blueprint->unsignedInteger('reserved_at')->nullable();
        $blueprint->unsignedInteger('available_at');
        $blueprint->unsignedInteger('created_at');
    });
}

/**
 * @param  list<Message>  $messages
 * @return list<string>
 */
function consumeSubscriptionReferences(array $messages): array
{
    return array_map(
        fn (Message $message): string => $message->payload['reference'],
        $messages,
    );
}

/**
 * @param  array<mixed>  $payload
 */
function consumeSubscriptionEnvelope(array $payload): string
{
    return json_encode([
        'id' => '01890a5d-ac96-774b-bcd0-48f622f3e798',
        'type' => 'order.created',
        'payload' => $payload,
        'published_at' => '2026-07-15T14:23:08.417Z',
    ], JSON_THROW_ON_ERROR);
}
