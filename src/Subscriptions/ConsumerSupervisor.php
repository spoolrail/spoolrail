<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Closure;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Support\Facades\Log;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Throwable;

class ConsumerSupervisor
{
    private const float LOOP_PAUSE_SECONDS = 0.1;

    private const int TERMINATION_POLL_INTERVAL_SECONDS = 1;

    private const int SHUTDOWN_TIMEOUT_SECONDS = 10;

    private ?int $stopSignal = null;

    public function __construct(
        private SubscriptionConsumer $consumer,
        private StartConsumerProcess $startProcess,
        private TerminationSignal $terminationSignal,
        private ConsumerConfig $config,
        private ExceptionHandler $exceptions,
    ) {}

    /**
     * @param  non-empty-list<string>  $subscriptionNames
     * @param  Closure(string, string): void  $writeOutput
     */
    public function supervise(array $subscriptionNames, Closure $writeOutput): bool
    {
        $processCount = $this->preflight($subscriptionNames);
        $generation = $this->terminationSignal->current();
        $consumers = array_map(
            static fn (array $subscriptionNames): SupervisedConsumer => new SupervisedConsumer($subscriptionNames),
            $this->assignments($subscriptionNames, $processCount),
        );
        $nextTerminationPollAt = $this->now() + self::TERMINATION_POLL_INTERVAL_SECONDS;

        while ($this->stopSignal === null) {
            $now = $this->now();

            foreach ($consumers as $consumer) {
                $this->monitor($consumer, $writeOutput, $now);
            }

            if ($now >= $nextTerminationPollAt) {
                $this->checkForTermination($generation);
                $nextTerminationPollAt = $now + self::TERMINATION_POLL_INTERVAL_SECONDS;
            }

            $this->pause();
        }

        return $this->stopAll($consumers, $this->stopSignal ?? SIGTERM);
    }

    public function stop(int $signal): void
    {
        $this->stopSignal ??= $signal;
    }

    /** @param  non-empty-list<string>  $subscriptionNames */
    private function preflight(array $subscriptionNames): int
    {
        $this->startProcess->ensureSupported();
        $processCount = $this->config->processes();
        $this->config->idleWaitMilliseconds();

        foreach ($subscriptionNames as $subscriptionName) {
            $this->consumer->ensureCanConsume($subscriptionName);
        }

        return $processCount;
    }

    /**
     * @param  non-empty-list<string>  $subscriptionNames
     * @return non-empty-list<non-empty-list<string>>
     */
    private function assignments(array $subscriptionNames, int $configuredProcessCount): array
    {
        $processCount = min($configuredProcessCount, count($subscriptionNames));
        $minimumSize = intdiv(count($subscriptionNames), $processCount);
        $largerAssignments = count($subscriptionNames) % $processCount;
        $assignments = [[$subscriptionNames[0]]];
        $assignmentIndex = 0;
        $assignmentSize = $minimumSize + ($largerAssignments > 0 ? 1 : 0);

        for ($index = 1, $count = count($subscriptionNames); $index < $count; $index++) {
            if (count($assignments[$assignmentIndex]) === $assignmentSize) {
                $assignmentIndex++;
                $assignmentSize = $minimumSize
                    + ($assignmentIndex < $largerAssignments ? 1 : 0);
                $assignments[$assignmentIndex] = [$subscriptionNames[$index]];

                continue;
            }

            $assignments[$assignmentIndex][] = $subscriptionNames[$index];
        }

        return array_values($assignments);
    }

    /** @param  Closure(string, string): void  $writeOutput */
    private function startWhenReady(
        SupervisedConsumer $consumer,
        Closure $writeOutput,
        float $now,
    ): void {
        if ($this->stopSignal !== null || ! $consumer->isReadyToStart($now)) {
            return;
        }

        try {
            $process = ($this->startProcess)($consumer->subscriptionNames, $writeOutput);
        } catch (Throwable $exception) {
            $this->report(ConsumerException::consumerProcessCouldNotStart(
                $consumer->subscriptionNames,
                $exception,
            ));
            $consumer->markAsFailed($this->now());

            return;
        }

        $consumer->markAsStarted($process, $this->now());
    }

    /** @param  Closure(string, string): void  $writeOutput */
    private function monitor(
        SupervisedConsumer $consumer,
        Closure $writeOutput,
        float $now,
    ): void {
        $process = $consumer->process();

        if (! $process instanceof ConsumerProcess) {
            $this->startWhenReady($consumer, $writeOutput, $now);

            return;
        }

        if ($process->isRunning()) {
            $consumer->resetBackoffWhenStable($now);

            return;
        }

        if (($exception = $process->unreportedFailure()) instanceof ConsumerException) {
            $this->report($exception);
        }

        $consumer->markAsFailed($this->now());
    }

    private function checkForTermination(?string $generation): void
    {
        try {
            $current = $this->terminationSignal->current();
        } catch (Throwable $exception) {
            $this->report(ConsumerException::terminationSignalCouldNotBeRead($exception));

            return;
        }

        if ($current !== $generation) {
            $this->stop(SIGTERM);
        }
    }

    /** @param  list<SupervisedConsumer>  $consumers */
    private function stopAll(array $consumers, int $signal): bool
    {
        $this->signalAll($consumers, $signal);
        $this->waitForProcessesToStop($consumers);

        return ! $this->killRemaining($consumers);
    }

    /** @param  list<SupervisedConsumer>  $consumers */
    private function signalAll(array $consumers, int $signal): void
    {
        foreach ($consumers as $consumer) {
            try {
                $consumer->process()?->signal($signal);
            } catch (Throwable $exception) {
                $this->logSignalFailure($consumer, $exception);
            }
        }
    }

    /** @param  list<SupervisedConsumer>  $consumers */
    private function waitForProcessesToStop(array $consumers): void
    {
        $deadline = $this->now() + self::SHUTDOWN_TIMEOUT_SECONDS;

        while ($this->hasRunningProcess($consumers) && $this->now() < $deadline) {
            $this->pause();
        }
    }

    /** @param  list<SupervisedConsumer>  $consumers */
    private function killRemaining(array $consumers): bool
    {
        $forced = false;

        foreach ($consumers as $consumer) {
            $process = $consumer->process();

            if ($process?->isRunning() !== true) {
                continue;
            }

            $forced = true;
            $process->kill();
            $this->logForcedShutdown($consumer);
        }

        return $forced;
    }

    /** @param  list<SupervisedConsumer>  $consumers */
    private function hasRunningProcess(array $consumers): bool
    {
        return array_any(
            $consumers,
            static fn (SupervisedConsumer $consumer): bool => $consumer->process()?->isRunning() === true,
        );
    }

    private function logForcedShutdown(SupervisedConsumer $consumer): void
    {
        try {
            Log::warning('Spoolrail forcefully stopped an unresponsive consumer process.', [
                'subscriptions' => $consumer->subscriptionNames,
            ]);
        } catch (Throwable) {
            // Shutdown must complete even when secondary logging fails.
        }
    }

    private function logSignalFailure(
        SupervisedConsumer $consumer,
        Throwable $exception,
    ): void {
        try {
            Log::warning('Spoolrail could not signal a consumer process during shutdown.', [
                'subscriptions' => $consumer->subscriptionNames,
                'exception' => $exception,
            ]);
        } catch (Throwable) {
            // Shutdown must continue to the forced cleanup deadline.
        }
    }

    private function report(ConsumerException $exception): void
    {
        try {
            $this->exceptions->report($exception);
        } catch (Throwable) {
            // Reporting must not prevent the supervisor from recovering a worker.
        }
    }

    protected function now(): float
    {
        return hrtime(true) / 1_000_000_000;
    }

    protected function pause(): void
    {
        usleep((int) (self::LOOP_PAUSE_SECONDS * 1_000_000));
    }
}
