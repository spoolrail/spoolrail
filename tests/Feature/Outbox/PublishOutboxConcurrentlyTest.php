<?php

declare(strict_types=1);

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Outbox\OutboxAssignment;
use Spoolrail\Spoolrail\Outbox\OutboxDispatcher;
use Spoolrail\Spoolrail\Outbox\PublishOutbox;
use Spoolrail\Spoolrail\Outbox\StartOutboxProcess;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithOutbox;
use Symfony\Component\Process\Process;

uses(InteractsWithOutbox::class);

beforeEach(function (): void {
    config()->set('spoolrail.publisher_retries.times', 0);
});

test('keeps serial dispatch in the parent process when concurrency is one', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.concurrency', 1);
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);

    $driver = Mockery::mock(Driver::class);
    $driver->allows('consume');
    $driver->expects('publish')->once();
    Spoolrail::extend('recording', static fn (): Driver => $driver);

    Spoolrail::connection('events')->publish(
        'orders',
        Message::make('order.created', ['order_id' => 42]),
    );

    $startProcess = Mockery::mock(StartOutboxProcess::class);
    $startProcess->shouldNotReceive('ensureSupported', '__invoke');
    app()->instance(StartOutboxProcess::class, $startProcess);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:publish')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect(DB::table('outbox_publications')->count())->toBe(0);
});

test('starts one worker for one active lane when concurrency exceeds one', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.concurrency', '4');
    stageOutboxLane('events', 'orders', 2);

    $publisher = Mockery::mock(PublishOutbox::class);
    $publisher->shouldNotReceive('__invoke');
    app()->instance(PublishOutbox::class, $publisher);

    $process = successfulOutboxProcess();
    $startProcess = Mockery::mock(StartOutboxProcess::class);
    $startProcess->expects('ensureSupported');
    $startProcess->expects('__invoke')->once()->andReturn($process);
    app()->instance(StartOutboxProcess::class, $startProcess);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:publish')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('assigns largest lane backlogs to the currently lightest worker', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.concurrency', 3);
    $orders = stageOutboxLane('events', 'orders', 8);
    $invoices = stageOutboxLane('billing', 'invoices', 7);
    $returns = stageOutboxLane('events', 'returns', 6);
    $refunds = stageOutboxLane('billing', 'refunds', 5);
    $shipments = stageOutboxLane('events', 'shipments', 5);

    $assignments = [];
    $startProcess = Mockery::mock(StartOutboxProcess::class);
    $startProcess->expects('ensureSupported');
    $startProcess->expects('__invoke')
        ->times(3)
        ->andReturnUsing(function (OutboxAssignment $assignment) use (&$assignments): Process {
            $assignments[] = $assignment->laneHeadIds;

            return successfulOutboxProcess();
        });
    app()->instance(StartOutboxProcess::class, $startProcess);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:publish')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($assignments)->toBe([
        [$orders],
        [$invoices, $shipments],
        [$returns, $refunds],
    ]);
});

test('waits for healthy workers without replacing a failed worker', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.concurrency', 2);
    stageOutboxLane('events', 'orders', 1);
    stageOutboxLane('events', 'returns', 1);

    $failed = Mockery::mock(Process::class);
    $failed->expects('wait')->once()->andReturn(1);

    $healthy = Mockery::mock(Process::class);
    $healthy->expects('wait')->once()->andReturn(0);

    $startProcess = Mockery::mock(StartOutboxProcess::class);
    $startProcess->expects('ensureSupported');
    $startProcess->expects('__invoke')->twice()->andReturn($failed, $healthy);
    app()->instance(StartOutboxProcess::class, $startProcess);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:publish')->run();

    // --- Assert ---
    expect($exitCode)->toBe(1);
});

test('forwards cooperative termination to every worker and waits for them', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.concurrency', 2);
    stageOutboxLane('events', 'orders', 1);
    stageOutboxLane('events', 'returns', 1);

    $dispatcher = null;
    $first = Mockery::mock(Process::class);
    $first->expects('isRunning')->once()->andReturnTrue();
    $first->expects('signal')->once()->with(SIGTERM);
    $first->expects('wait')
        ->once()
        ->andReturnUsing(function () use (&$dispatcher): int {
            $dispatcher->stop(SIGTERM);

            return 0;
        });

    $second = Mockery::mock(Process::class);
    $second->expects('isRunning')->once()->andReturnTrue();
    $second->expects('signal')->once()->with(SIGTERM);
    $second->expects('wait')->once()->andReturn(0);

    $startProcess = Mockery::mock(StartOutboxProcess::class);
    $startProcess->expects('ensureSupported');
    $startProcess->expects('__invoke')->twice()->andReturn($first, $second);

    $dispatcher = new OutboxDispatcher(
        app('config'),
        app(PublishOutbox::class),
        $startProcess,
        app(ExceptionHandler::class),
    );

    // --- Act ---
    $succeeded = $dispatcher(static function (): void {});

    // --- Assert ---
    expect($succeeded)->toBeTrue();
});

test('rejects invalid concurrency before discovering lanes or starting workers', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.concurrency', '2workers');
    Schema::drop('outbox_publications');

    $startProcess = Mockery::mock(StartOutboxProcess::class);
    $startProcess->shouldNotReceive('ensureSupported', '__invoke');
    app()->instance(StartOutboxProcess::class, $startProcess);

    // --- Act / Assert ---
    expect(fn () => $this->artisan('spoolrail:publish')->run())
        ->toThrow(
            InvalidConfigException::class,
            'Spoolrail outbox concurrency must be a positive integer.',
        );
});

function stageOutboxLane(string $connection, string $topic, int $size): int
{
    config()->set("spoolrail.connections.$connection", ['driver' => 'array']);

    foreach (range(1, $size) as $sequence) {
        Spoolrail::connection($connection)->publish(
            $topic,
            Message::make('test.message', ['sequence' => $sequence]),
        );
    }

    return (int) DB::table('outbox_publications')
        ->where('connection', $connection)
        ->where('topic', $topic)
        ->min('id');
}

function successfulOutboxProcess(): Process
{
    $process = Mockery::mock(Process::class);
    $process->expects('wait')->once()->andReturn(0);

    return $process;
}
