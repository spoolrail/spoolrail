<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

class SupervisedSubscription
{
    private const array RESTART_DELAYS = [1, 5, 15, 30, 60];

    private const int STABILITY_SECONDS = 60;

    private ?SubscriptionProcess $process = null;

    private int $consecutiveFailures = 0;

    private float $restartAt = 0;

    private ?float $startedAt = null;

    public function __construct(
        public readonly string $name,
    ) {}

    public function process(): ?SubscriptionProcess
    {
        return $this->process;
    }

    public function markAsStarted(SubscriptionProcess $process, float $now): void
    {
        $this->process = $process;
        $this->startedAt = $now;
    }

    public function markAsFailed(float $now): void
    {
        $this->process = null;
        $this->startedAt = null;
        $this->consecutiveFailures++;

        $delay = self::RESTART_DELAYS[
            min($this->consecutiveFailures - 1, count(self::RESTART_DELAYS) - 1)
        ];
        $this->restartAt = $now + $delay;
    }

    public function isReadyToStart(float $now): bool
    {
        return ! $this->process instanceof SubscriptionProcess && $now >= $this->restartAt;
    }

    public function resetBackoffWhenStable(float $now): bool
    {
        if ($this->consecutiveFailures === 0
            || $this->startedAt === null
            || $now - $this->startedAt < self::STABILITY_SECONDS) {
            return false;
        }

        $this->consecutiveFailures = 0;

        return true;
    }
}
