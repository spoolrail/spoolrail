<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Closure;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Symfony\Component\Process\Process;

/** @internal */
class ConsumerProcess
{
    public const int REPORTED_FAILURE_EXIT_CODE = 70;

    /** @param  non-empty-list<string>  $subscriptionNames */
    public function __construct(
        private array $subscriptionNames,
        private Process $process,
    ) {}

    /** @param  Closure(string, string): void  $writeOutput */
    public function start(Closure $writeOutput): void
    {
        $this->process->start(
            function (string $type, string $output) use ($writeOutput): void {
                $writeOutput(implode(',', $this->subscriptionNames), $output);

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

        $reason = $this->process->hasBeenSignaled()
            ? "was terminated by signal [{$this->process->getTermSignal()}]"
            : "exited with code [{$this->process->getExitCode()}]";

        return ConsumerException::consumerProcessExitedUnexpectedly(
            $this->subscriptionNames,
            $reason,
        );
    }
}
