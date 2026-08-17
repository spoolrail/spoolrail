<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Outbox;

use Closure;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Symfony\Component\Process\Process;
use Throwable;

class OutboxDispatcher
{
    private bool $usesWorkers = false;

    private ?int $stopSignal = null;

    /**
     * @var list<Process>
     */
    private array $workers = [];

    public function __construct(
        private Repository $config,
        private PublishOutbox $publisher,
        private StartOutboxProcess $startProcess,
        private ExceptionHandler $exceptions,
    ) {}

    /**
     * @param  Closure(string): void  $writeOutput
     */
    public function __invoke(Closure $writeOutput): bool
    {
        $concurrencyLimit = $this->concurrencyLimit();
        $this->usesWorkers = $concurrencyLimit > 1;
        $highestPublicationId = OutboxPublication::highestId();

        if ($highestPublicationId === null) {
            return true;
        }

        $lanes = OutboxPublication::lanesThrough($highestPublicationId);

        if ($lanes === []) {
            return true;
        }

        if (! $this->usesWorkers) {
            return ($this->publisher)(OutboxAssignment::fromLanes($highestPublicationId, $lanes));
        }

        $this->startProcess->ensureSupported();

        return $this->supervise(
            $this->assignLanes($highestPublicationId, $lanes, $concurrencyLimit),
            $writeOutput,
        );
    }

    public function stop(int $signal): void
    {
        $signal = $this->stopSignal ?? $signal;
        $this->stopSignal = $signal;

        if (! $this->usesWorkers) {
            $this->publisher->stop();

            return;
        }

        foreach ($this->workers as $worker) {
            $this->signal($worker, $signal);
        }
    }

    /**
     * @param  non-empty-list<OutboxLane>  $lanes
     * @return non-empty-list<OutboxAssignment>
     */
    private function assignLanes(int $highestPublicationId, array $lanes, int $concurrencyLimit): array
    {
        usort(
            $lanes,
            static fn (OutboxLane $left, OutboxLane $right): int => $right->backlogSize <=> $left->backlogSize
                ?: $left->headId <=> $right->headId,
        );

        $workerCount = min($concurrencyLimit, count($lanes));
        $workerLanes = array_fill(0, $workerCount, []);
        $workerLoads = array_fill(0, $workerCount, 0);

        foreach ($lanes as $lane) {
            $workerIndex = 0;

            for ($candidateIndex = 1; $candidateIndex < $workerCount; $candidateIndex++) {
                if ($workerLoads[$candidateIndex] < $workerLoads[$workerIndex]) {
                    $workerIndex = $candidateIndex;
                }
            }

            $workerLanes[$workerIndex][] = $lane;
            $workerLoads[$workerIndex] += $lane->backlogSize;
        }

        return array_map(
            static fn (array $lanes): OutboxAssignment => OutboxAssignment::fromLanes($highestPublicationId, $lanes),
            $workerLanes,
        );
    }

    /**
     * @param  non-empty-list<OutboxAssignment>  $assignments
     * @param  Closure(string): void  $writeOutput
     */
    private function supervise(array $assignments, Closure $writeOutput): bool
    {
        $startSucceeded = $this->startWorkers($assignments, $writeOutput);
        $workersSucceeded = $this->waitForWorkers();

        return $startSucceeded && $workersSucceeded;
    }

    /**
     * @param  non-empty-list<OutboxAssignment>  $assignments
     * @param  Closure(string): void  $writeOutput
     */
    private function startWorkers(array $assignments, Closure $writeOutput): bool
    {
        $succeeded = true;

        foreach ($assignments as $assignment) {
            if ($this->stopSignal !== null) {
                break;
            }

            try {
                $this->startWorker($assignment, $writeOutput);
            } catch (Throwable $exception) {
                $this->report($exception);
                $succeeded = false;
            }
        }

        return $succeeded;
    }

    /**
     * @param  Closure(string): void  $writeOutput
     */
    private function startWorker(OutboxAssignment $assignment, Closure $writeOutput): void
    {
        $worker = ($this->startProcess)($assignment, $writeOutput);
        $this->workers[] = $worker;
        $this->signalIfStopping($worker);
    }

    private function waitForWorkers(): bool
    {
        $succeeded = true;

        foreach ($this->workers as $worker) {
            try {
                if ($worker->wait() !== 0) {
                    $succeeded = false;
                }
            } catch (Throwable $exception) {
                $this->report($exception);
                $succeeded = false;
            }
        }

        return $succeeded;
    }

    private function concurrencyLimit(): int
    {
        $concurrency = $this->config->get('spoolrail.outbox.concurrency', 1);

        if (is_string($concurrency)) {
            $concurrency = filter_var($concurrency, FILTER_VALIDATE_INT);
        }

        if (! is_int($concurrency) || $concurrency < 1) {
            throw InvalidConfigException::invalidOutboxConcurrency();
        }

        return $concurrency;
    }

    private function report(Throwable $exception): void
    {
        try {
            $this->exceptions->report($exception);
        } catch (Throwable) {
            // Reporting must not interrupt sibling outbox workers.
        }
    }

    private function signalIfStopping(Process $worker): void
    {
        $signal = $this->stopSignal;

        if ($signal !== null) {
            $this->signal($worker, $signal);
        }
    }

    private function signal(Process $worker, int $signal): void
    {
        try {
            if ($worker->isRunning()) {
                $worker->signal($signal);
            }
        } catch (Throwable) {
            // A worker may finish between the running check and the signal.
        }
    }
}
