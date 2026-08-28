<?php

declare(strict_types=1);

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Support\Facades\Log;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Subscriptions\ConsumerConfig;
use Spoolrail\Spoolrail\Subscriptions\ConsumerProcess;
use Spoolrail\Spoolrail\Subscriptions\ConsumerSupervisor;
use Spoolrail\Spoolrail\Subscriptions\StartConsumerProcess;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Spoolrail\Spoolrail\Subscriptions\TerminationSignal;
use Symfony\Component\Process\Process;

test('validates every selected subscription before starting a child', function (): void {
    // --- Arrange ---
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('ensureCanConsume')->with('warehouse-orders');
    $consumer->expects('ensureCanConsume')
        ->with('billing-orders')
        ->andThrow(new LogicException('Invalid Queue target.'));

    $start = Mockery::mock(StartConsumerProcess::class);
    $start->expects('ensureSupported');
    $start->shouldNotReceive('__invoke');
    $config = Mockery::mock(ConsumerConfig::class);
    $config->expects('processes')->once()->andReturn(1);
    $config->expects('idleWaitMilliseconds')->once()->andReturn(100);
    $termination = Mockery::mock(TerminationSignal::class);
    $termination->shouldNotReceive('current');
    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldNotReceive('report');
    $supervisor = new ConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $config,
        $exceptions,
    );

    // --- Act / Assert ---
    expect(fn (): bool => $supervisor->supervise(
        ['warehouse-orders', 'billing-orders'],
        static function (): void {},
    ))->toThrow(LogicException::class, 'Invalid Queue target.');
});

test('assigns subscriptions evenly to the configured number of static children', function (): void {
    // --- Arrange ---
    $subscriptions = array_map(
        static fn (int $number): string => "orders-$number",
        range(1, 14),
    );
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    foreach ($subscriptions as $subscription) {
        $consumer->expects('ensureCanConsume')->with($subscription);
    }

    $startedGroups = [];
    $start = Mockery::mock(StartConsumerProcess::class);
    $start->expects('ensureSupported');
    $start->expects('__invoke')
        ->times(3)
        ->andReturnUsing(function (array $group) use (&$startedGroups): ConsumerProcess {
            $startedGroups[] = $group;

            return runningConsumerProcess($group);
        });
    $config = Mockery::mock(ConsumerConfig::class);
    $config->allows('processes')->andReturn(3);
    $config->allows('idleWaitMilliseconds')->andReturn(100);
    $termination = Mockery::mock(TerminationSignal::class);
    $termination->expects('current')->twice()->andReturn('before', 'after');
    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldNotReceive('report');
    $supervisor = controlledConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $config,
        $exceptions,
    );

    // --- Act ---
    $supervisor->supervise($subscriptions, static function (): void {});

    // --- Assert ---
    expect(array_map(count(...), $startedGroups))->toBe([5, 5, 4]);
    expect(array_merge(...$startedGroups))->toBe($subscriptions);
});

test('starts no empty children when process capacity exceeds subscriptions', function (): void {
    // --- Arrange ---
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('ensureCanConsume')->with('warehouse-orders');
    $consumer->expects('ensureCanConsume')->with('billing-orders');
    $groups = [];
    $start = Mockery::mock(StartConsumerProcess::class);
    $start->expects('ensureSupported');
    $start->expects('__invoke')
        ->twice()
        ->andReturnUsing(function (array $group) use (&$groups): ConsumerProcess {
            $groups[] = $group;

            return runningConsumerProcess($group);
        });
    $config = Mockery::mock(ConsumerConfig::class);
    $config->allows('processes')->andReturn(5);
    $config->allows('idleWaitMilliseconds')->andReturn(100);
    $termination = Mockery::mock(TerminationSignal::class);
    $termination->expects('current')->twice()->andReturn('before', 'after');
    $exceptions = Mockery::mock(ExceptionHandler::class);
    $supervisor = controlledConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $config,
        $exceptions,
    );

    // --- Act ---
    $supervisor->supervise(
        ['warehouse-orders', 'billing-orders'],
        static function (): void {},
    );

    // --- Assert ---
    expect($groups)->toBe([
        ['warehouse-orders'],
        ['billing-orders'],
    ]);
});

test('restarts a failed group with the same assignment while its sibling stays running', function (): void {
    // --- Arrange ---
    $subscriptions = [
        'warehouse-orders',
        'billing-orders',
        'catalog-orders',
    ];
    $processFailure = ConsumerException::consumerProcessExitedUnexpectedly(
        ['warehouse-orders', 'billing-orders'],
        'exited with code [1]',
    );
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    foreach ($subscriptions as $subscription) {
        $consumer->expects('ensureCanConsume')->with($subscription);
    }

    $starts = [];
    $start = Mockery::mock(StartConsumerProcess::class);
    $start->expects('ensureSupported');
    $start->allows('__invoke')->andReturnUsing(
        function (array $group) use (&$starts, $processFailure): ConsumerProcess {
            $label = implode(',', $group);
            $starts[$label] = ($starts[$label] ?? 0) + 1;

            if ($label === 'warehouse-orders,billing-orders' && $starts[$label] === 1) {
                return stoppedConsumerProcess($group, $processFailure);
            }

            return runningConsumerProcess($group);
        },
    );
    $config = Mockery::mock(ConsumerConfig::class);
    $config->allows('processes')->andReturn(2);
    $config->allows('idleWaitMilliseconds')->andReturn(100);
    $termination = Mockery::mock(TerminationSignal::class);
    $termination->expects('current')->times(3)->andReturn('before', 'before', 'after');
    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->expects('report')->once()->with($processFailure);
    $supervisor = controlledConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $config,
        $exceptions,
    );

    // --- Act ---
    $supervisor->supervise($subscriptions, static function (): void {});

    // --- Assert ---
    expect($starts)->toBe([
        'warehouse-orders,billing-orders' => 2,
        'catalog-orders' => 1,
    ]);
});

test('kills unresponsive groups after one shared shutdown deadline', function (): void {
    // --- Arrange ---
    $subscriptions = ['warehouse-orders', 'billing-orders'];
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    foreach ($subscriptions as $subscription) {
        $consumer->expects('ensureCanConsume')->with($subscription);
    }

    $supervisor = null;
    $signaledAt = [];
    $killedAt = [];
    $start = Mockery::mock(StartConsumerProcess::class);
    $start->expects('ensureSupported');
    $start->expects('__invoke')
        ->twice()
        ->andReturnUsing(function (array $group) use (
            &$supervisor,
            &$signaledAt,
            &$killedAt,
        ): ConsumerProcess {
            $label = implode(',', $group);

            return unresponsiveConsumerProcess(
                $group,
                function () use (&$supervisor, &$signaledAt, $label): void {
                    assert($supervisor instanceof ControlledConsumerSupervisor);
                    $signaledAt[$label] = $supervisor->time();
                },
                function () use (&$supervisor, &$killedAt, $label): void {
                    assert($supervisor instanceof ControlledConsumerSupervisor);
                    $killedAt[$label] = $supervisor->time();
                },
            );
        });
    $config = Mockery::mock(ConsumerConfig::class);
    $config->allows('processes')->andReturn(2);
    $config->allows('idleWaitMilliseconds')->andReturn(100);
    $termination = Mockery::mock(TerminationSignal::class);
    $termination->expects('current')->twice()->andReturn('before', 'after');
    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldNotReceive('report');
    Log::shouldReceive('warning')->twice();
    $supervisor = controlledConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $config,
        $exceptions,
    );

    // --- Act ---
    $clean = $supervisor->supervise($subscriptions, static function (): void {});

    // --- Assert ---
    expect($clean)->toBeFalse();
    expect(array_keys($signaledAt))->toBe([
        'warehouse-orders',
        'billing-orders',
    ]);
    expect(array_keys($killedAt))->toBe([
        'warehouse-orders',
        'billing-orders',
    ]);
    expect($signaledAt['warehouse-orders'])->toBe($signaledAt['billing-orders']);
    expect($killedAt['warehouse-orders'])->toBe($killedAt['billing-orders']);
    expect($killedAt['warehouse-orders'] - $signaledAt['warehouse-orders'])->toBe(10.0);
});

test('rejects invalid consumer settings without starting children', function (string $setting): void {
    config()->set("spoolrail.consumer.$setting", 0);
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->allows('ensureCanConsume');
    $start = Mockery::mock(StartConsumerProcess::class);
    $start->allows('ensureSupported');
    $start->shouldNotReceive('__invoke');
    $termination = Mockery::mock(TerminationSignal::class);
    $termination->shouldNotReceive('current');
    $exceptions = Mockery::mock(ExceptionHandler::class);
    $supervisor = new ConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        app(ConsumerConfig::class),
        $exceptions,
    );

    expect(fn (): bool => $supervisor->supervise(
        ['warehouse-orders'],
        static function (): void {},
    ))->toThrow(
        InvalidConfigException::class,
        "consumer setting [$setting] must be a positive integer",
    );
})->with([
    'process count' => 'processes',
    'idle wait' => 'idle_wait_milliseconds',
]);

/** @param  non-empty-list<string>  $subscriptionNames */
function runningConsumerProcess(array $subscriptionNames): ConsumerProcess
{
    return new class($subscriptionNames, new Process(['true'])) extends ConsumerProcess
    {
        private bool $running = true;

        public function isRunning(): bool
        {
            return $this->running;
        }

        public function signal(int $signal): void
        {
            $this->running = false;
        }

        public function kill(): void
        {
            $this->running = false;
        }
    };
}

/** @param  non-empty-list<string>  $subscriptionNames */
function stoppedConsumerProcess(
    array $subscriptionNames,
    ?ConsumerException $failure = null,
): ConsumerProcess {
    return new class($subscriptionNames, new Process(['true']), $failure) extends ConsumerProcess
    {
        public function __construct(
            array $subscriptionNames,
            Process $process,
            private ?ConsumerException $failure,
        ) {
            parent::__construct($subscriptionNames, $process);
        }

        public function isRunning(): bool
        {
            return false;
        }

        public function unreportedFailure(): ?ConsumerException
        {
            return $this->failure;
        }
    };
}

/**
 * @param  non-empty-list<string>  $subscriptionNames
 * @param  Closure(): void  $signal
 * @param  Closure(): void  $kill
 */
function unresponsiveConsumerProcess(
    array $subscriptionNames,
    Closure $signal,
    Closure $kill,
): ConsumerProcess {
    return new class($subscriptionNames, $signal, $kill) extends ConsumerProcess
    {
        public function __construct(
            array $subscriptionNames,
            private Closure $signalProcess,
            private Closure $killProcess,
        ) {
            parent::__construct($subscriptionNames, new Process(['true']));
        }

        public function isRunning(): bool
        {
            return true;
        }

        public function signal(int $signal): void
        {
            ($this->signalProcess)();
        }

        public function kill(): void
        {
            ($this->killProcess)();
        }
    };
}

function controlledConsumerSupervisor(
    SubscriptionConsumer $consumer,
    StartConsumerProcess $start,
    TerminationSignal $termination,
    ConsumerConfig $config,
    ExceptionHandler $exceptions,
): ControlledConsumerSupervisor {
    return new ControlledConsumerSupervisor(
        $consumer,
        $start,
        $termination,
        $config,
        $exceptions,
    );
}

class ControlledConsumerSupervisor extends ConsumerSupervisor
{
    private float $time = 0;

    public function time(): float
    {
        return $this->time;
    }

    #[Override]
    protected function now(): float
    {
        return $this->time;
    }

    #[Override]
    protected function pause(): void
    {
        $this->time++;
    }
}
