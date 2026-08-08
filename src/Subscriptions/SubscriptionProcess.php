<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Closure;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Symfony\Component\Process\Process;

class SubscriptionProcess
{
    public const int REPORTED_FAILURE_EXIT_CODE = 70;

    public function __construct(
        private string $subscription,
        private Process $process,
    ) {}

    /**
     * @param  Closure(string, string): void  $writeOutput
     */
    public function start(Closure $writeOutput): void
    {
        $this->process->start(
            function (string $type, string $output) use ($writeOutput): void {
                $writeOutput($this->subscription, $output);

                if ($type === Process::OUT) {
                    $this->process->clearOutput();
                } else {
                    $this->process->clearErrorOutput();
                }
            },
        );
    }

    public function isRunning(): bool
    {
        return $this->process->isRunning();
    }

    public function signal(int $signal): void
    {
        if ($this->process->isRunning()) {
            $this->process->signal($signal);
        }
    }

    public function kill(): void
    {
        $this->process->stop(0, SIGKILL);
    }

    public function unreportedFailure(): ?ConsumerException
    {
        if (! $this->process->hasBeenSignaled()
            && $this->process->getExitCode() === self::REPORTED_FAILURE_EXIT_CODE) {
            return null;
        }

        if ($this->process->hasBeenSignaled()) {
            $reason = "was terminated by signal [{$this->process->getTermSignal()}]";
        } else {
            $reason = "exited with code [{$this->process->getExitCode()}]";
        }

        return ConsumerException::subscriptionProcessExitedUnexpectedly(
            $this->subscription,
            $reason,
        );
    }
}
