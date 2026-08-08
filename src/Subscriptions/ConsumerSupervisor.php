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

    private const int TERMINATION_POLL_SECONDS = 1;

    private const int SHUTDOWN_SECONDS = 10;

    private ?int $stopSignal = null;

    public function __construct(
        private SubscriptionConsumer $consumer,
        private StartSubscriptionProcess $startProcess,
        private TerminationSignal $terminationSignal,
        private ExceptionHandler $exceptions,
    ) {}

    /**
     * @param  list<string>  $subscriptionNames
     * @param  Closure(string, string): void  $writeOutput
     */
    public function supervise(array $subscriptionNames, Closure $writeOutput): bool
    {
        $this->ensureCanSupervise($subscriptionNames);
        $generation = $this->terminationSignal->current();
        $subscriptions = array_map(
            static fn (string $name): SupervisedSubscription => new SupervisedSubscription($name),
            $subscriptionNames,
        );
        $nextTerminationPoll = $this->now() + self::TERMINATION_POLL_SECONDS;

        while ($this->stopSignal === null) {
            $now = $this->now();

            foreach ($subscriptions as $subscription) {
                $this->monitor($subscription, $writeOutput, $now);
            }

            if ($now >= $nextTerminationPoll) {
                $this->checkForTermination($generation);
                $nextTerminationPoll = $now + self::TERMINATION_POLL_SECONDS;
            }

            $this->pause();
        }

        return $this->stopAll($subscriptions, $this->stopSignal ?? SIGTERM);
    }

    public function stop(int $signal): void
    {
        $this->stopSignal ??= $signal;
    }

    /**
     * @param  list<string>  $subscriptionNames
     */
    private function ensureCanSupervise(array $subscriptionNames): void
    {
        $this->startProcess->ensureSupported();

        foreach ($subscriptionNames as $subscription) {
            $this->consumer->ensureCanConsume($subscription);
        }
    }

    /**
     * @param  Closure(string, string): void  $writeOutput
     */
    private function startWhenReady(
        SupervisedSubscription $subscription,
        Closure $writeOutput,
        float $now,
    ): void {
        if ($this->stopSignal !== null || ! $subscription->isReadyToStart($now)) {
            return;
        }

        try {
            $process = ($this->startProcess)($subscription->name, $writeOutput);
        } catch (Throwable $exception) {
            $this->report(ConsumerException::subscriptionProcessCouldNotStart(
                $subscription->name,
                $exception,
            ));
            $subscription->markAsFailed($now);

            return;
        }

        $subscription->markAsStarted($process, $now);
    }

    /**
     * @param  Closure(string, string): void  $writeOutput
     */
    private function monitor(
        SupervisedSubscription $subscription,
        Closure $writeOutput,
        float $now,
    ): void {
        $process = $subscription->process();

        if (! $process instanceof SubscriptionProcess) {
            $this->startWhenReady($subscription, $writeOutput, $now);

            return;
        }

        if ($process->isRunning()) {
            if ($subscription->resetBackoffWhenStable($now)) {
                $this->logRecovery($subscription->name);
            }

            return;
        }

        if (($exception = $process->unreportedFailure()) instanceof ConsumerException) {
            $this->report($exception);
        }

        $subscription->markAsFailed($now);
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

    /**
     * @param  list<SupervisedSubscription>  $subscriptions
     */
    private function stopAll(array $subscriptions, int $signal): bool
    {
        $this->signalAll($subscriptions, $signal);
        $this->waitForProcessesToStop($subscriptions);

        return ! $this->killRemaining($subscriptions);
    }

    /**
     * @param  list<SupervisedSubscription>  $subscriptions
     */
    private function signalAll(array $subscriptions, int $signal): void
    {
        foreach ($subscriptions as $subscription) {
            try {
                $subscription->process()?->signal($signal);
            } catch (Throwable $exception) {
                $this->logSignalFailure($subscription->name, $exception);
            }
        }
    }

    /**
     * @param  list<SupervisedSubscription>  $subscriptions
     */
    private function waitForProcessesToStop(array $subscriptions): void
    {
        $deadline = $this->now() + self::SHUTDOWN_SECONDS;

        while ($this->hasRunningProcess($subscriptions) && $this->now() < $deadline) {
            $this->pause();
        }
    }

    /**
     * @param  list<SupervisedSubscription>  $subscriptions
     */
    private function killRemaining(array $subscriptions): bool
    {
        $forced = false;

        foreach ($subscriptions as $subscription) {
            $process = $subscription->process();

            if ($process?->isRunning() !== true) {
                continue;
            }

            $forced = true;
            $process->kill();
            $this->logForcedShutdown($subscription->name);
        }

        return $forced;
    }

    /**
     * @param  list<SupervisedSubscription>  $subscriptions
     */
    private function hasRunningProcess(array $subscriptions): bool
    {
        return array_any($subscriptions, fn (SupervisedSubscription $subscription): bool => $subscription->process()?->isRunning() === true);
    }

    private function logForcedShutdown(string $subscription): void
    {
        try {
            Log::warning('Spoolrail forcefully stopped an unresponsive consumer process.', [
                'subscription' => $subscription,
            ]);
        } catch (Throwable) {
            // Shutdown must complete even when secondary logging fails.
        }
    }

    private function logRecovery(string $subscription): void
    {
        try {
            Log::notice('Spoolrail subscription recovered.', [
                'subscription' => $subscription,
            ]);
        } catch (Throwable) {
            // Recovery reporting must not interrupt a healthy subscription.
        }
    }

    private function logSignalFailure(string $subscription, Throwable $exception): void
    {
        try {
            Log::warning('Spoolrail could not signal a consumer process during shutdown.', [
                'subscription' => $subscription,
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
