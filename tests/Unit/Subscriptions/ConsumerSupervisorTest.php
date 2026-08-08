<?php

declare(strict_types=1);

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Support\Facades\Log;
use Spoolrail\Spoolrail\Subscriptions\ConsumerSupervisor;
use Spoolrail\Spoolrail\Subscriptions\StartSubscriptionProcess;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionProcess;
use Spoolrail\Spoolrail\Subscriptions\TerminationSignal;

test('validates every selected subscription before starting a child', function (): void {
    // --- Arrange ---
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('ensureCanConsume')->with('warehouse-orders');
    $consumer->expects('ensureCanConsume')
        ->with('billing-orders')
        ->andThrow(new LogicException('Invalid Queue target.'));

    $start = Mockery::mock(StartSubscriptionProcess::class);
    $start->expects('ensureSupported');
    $start->shouldNotReceive('__invoke');

    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldNotReceive('report');

    $termination = Mockery::mock(TerminationSignal::class);
    $termination->shouldNotReceive('current');

    $supervisor = new ConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $exceptions,
    );

    // --- Act / Assert ---
    expect(fn (): bool => $supervisor->supervise(
        ['warehouse-orders', 'billing-orders'],
        static function (): void {},
    ))->toThrow(LogicException::class, 'Invalid Queue target.');
});

test('starts a sibling while another child remains running', function (): void {
    // --- Arrange ---
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('ensureCanConsume')->with('blocked-orders');
    $consumer->expects('ensureCanConsume')->with('healthy-orders');

    $termination = Mockery::mock(TerminationSignal::class);
    $termination->expects('current')->twice()->andReturn('before', 'after');

    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldNotReceive('report');

    $supervisor = new ConsumerSupervisor(
        $consumer,
        new StartSubscriptionProcess(app(), dirname(__DIR__, 2).'/Fixtures/processes/long-running-consumer.php'),
        $termination,
        $exceptions,
    );
    $output = [];

    // --- Act ---
    $clean = $supervisor->supervise(
        ['blocked-orders', 'healthy-orders'],
        function (string $subscription, string $chunk) use (&$output): void {
            $output[$subscription] = ($output[$subscription] ?? '').$chunk;
        },
    );

    // --- Assert ---
    expect($clean)->toBeTrue();
    expect($output['healthy-orders'] ?? null)
        ->toContain('spoolrail:consume healthy-orders');
});

test('restarts only a failed subscription while its sibling keeps running', function (): void {
    // --- Arrange ---
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('ensureCanConsume')->with('warehouse-orders');
    $consumer->expects('ensureCanConsume')->with('billing-orders');

    $failedWarehouse = Mockery::mock(SubscriptionProcess::class);
    $failedWarehouse->expects('isRunning')->once()->andReturnFalse();
    $failedWarehouse->expects('unreportedFailure')->once()->andReturnNull();

    $replacementWarehouse = Mockery::mock(SubscriptionProcess::class);
    $replacementWarehouse->expects('signal')->with(SIGTERM)->once();
    $replacementWarehouse->expects('isRunning')->twice()->andReturnFalse();

    $billing = Mockery::mock(SubscriptionProcess::class);
    $billing->expects('isRunning')->times(4)->andReturn(true, true, false, false);
    $billing->expects('signal')->with(SIGTERM)->once();

    $starts = ['warehouse-orders' => 0, 'billing-orders' => 0];
    $start = Mockery::mock(StartSubscriptionProcess::class);
    $start->expects('ensureSupported');
    $start->allows('__invoke')->andReturnUsing(
        function (string $subscription) use (
            &$starts,
            $failedWarehouse,
            $replacementWarehouse,
            $billing,
        ): SubscriptionProcess {
            $starts[$subscription]++;

            return match ([$subscription, $starts[$subscription]]) {
                ['warehouse-orders', 1] => $failedWarehouse,
                ['warehouse-orders', 2] => $replacementWarehouse,
                ['billing-orders', 1] => $billing,
                default => throw new LogicException('Unexpected process start.'),
            };
        },
    );

    $termination = Mockery::mock(TerminationSignal::class);
    $termination->expects('current')->times(3)->andReturn('before', 'before', 'after');

    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldNotReceive('report');

    $supervisor = controlledConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $exceptions,
    );

    // --- Act ---
    $clean = $supervisor->supervise(
        ['warehouse-orders', 'billing-orders'],
        static function (): void {},
    );

    // --- Assert ---
    expect($clean)->toBeTrue();
    expect($starts)->toBe(['warehouse-orders' => 2, 'billing-orders' => 1]);
});

test('logs recovery once after a failed subscription remains active for sixty seconds', function (): void {
    // --- Arrange ---
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('ensureCanConsume')->with('warehouse-orders');

    $failed = Mockery::mock(SubscriptionProcess::class);
    $failed->expects('isRunning')->once()->andReturnFalse();
    $failed->expects('unreportedFailure')->once()->andReturnNull();

    $supervisor = null;
    $replacement = Mockery::mock(SubscriptionProcess::class);
    $replacement->allows('isRunning')->andReturnUsing(
        function () use (&$supervisor): bool {
            return $supervisor->time() < 63;
        },
    );
    $replacement->expects('signal')->with(SIGTERM)->once();

    $start = Mockery::mock(StartSubscriptionProcess::class);
    $start->expects('ensureSupported');
    $start->expects('__invoke')->twice()->andReturn($failed, $replacement);

    $termination = Mockery::mock(TerminationSignal::class);
    $termination->allows('current')->andReturnUsing(
        function () use (&$supervisor): string {
            return $supervisor->time() >= 62 ? 'after' : 'before';
        },
    );

    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldNotReceive('report');

    Log::shouldReceive('notice')
        ->once()
        ->with('Spoolrail subscription recovered.', [
            'subscription' => 'warehouse-orders',
        ]);

    $supervisor = controlledConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $exceptions,
    );

    // --- Act ---
    $clean = $supervisor->supervise(
        ['warehouse-orders'],
        static function (): void {},
    );

    // --- Assert ---
    expect($clean)->toBeTrue();
});

test('kills unresponsive children after one shared shutdown deadline', function (): void {
    // --- Arrange ---
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('ensureCanConsume')->with('warehouse-orders');
    $consumer->expects('ensureCanConsume')->with('billing-orders');

    $supervisor = null;
    $signaledAt = [];
    $killedAt = [];
    $processes = [];

    foreach (['warehouse-orders', 'billing-orders'] as $subscription) {
        $process = Mockery::mock(SubscriptionProcess::class);
        $process->allows('isRunning')->andReturnTrue();
        $process->expects('signal')
            ->with(SIGTERM)
            ->once()
            ->andReturnUsing(function () use (&$signaledAt, &$supervisor, $subscription): void {
                $signaledAt[$subscription] = $supervisor->time();
            });
        $process->expects('kill')
            ->once()
            ->andReturnUsing(function () use (&$killedAt, &$supervisor, $subscription): void {
                $killedAt[$subscription] = $supervisor->time();
            });
        $processes[$subscription] = $process;
    }

    $start = Mockery::mock(StartSubscriptionProcess::class);
    $start->expects('ensureSupported');
    $start->expects('__invoke')
        ->twice()
        ->andReturnUsing(fn (string $subscription): SubscriptionProcess => $processes[$subscription]);

    $termination = Mockery::mock(TerminationSignal::class);
    $termination->expects('current')->twice()->andReturn('before', 'after');

    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldNotReceive('report');

    $supervisor = controlledConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $exceptions,
    );

    // --- Act ---
    $clean = $supervisor->supervise(
        ['warehouse-orders', 'billing-orders'],
        static function (): void {},
    );

    // --- Assert ---
    expect($clean)->toBeFalse();
    expect($signaledAt['billing-orders'])->toBe($signaledAt['warehouse-orders']);
    expect($killedAt['billing-orders'])->toBe($killedAt['warehouse-orders']);
    expect($killedAt['warehouse-orders'] - $signaledAt['warehouse-orders'])->toBe(10.0);
});

function controlledConsumerSupervisor(
    SubscriptionConsumer $consumer,
    StartSubscriptionProcess $start,
    TerminationSignal $termination,
    ExceptionHandler $exceptions,
): ConsumerSupervisor {
    return new class($consumer, $start, $termination, $exceptions) extends ConsumerSupervisor
    {
        private float $time = 0;

        public function time(): float
        {
            return $this->time;
        }

        protected function now(): float
        {
            return $this->time;
        }

        protected function pause(): void
        {
            $this->time++;
        }
    };
}
