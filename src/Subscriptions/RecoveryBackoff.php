<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

/** @internal */
class RecoveryBackoff
{
    private const array DELAYS = [1, 5, 15, 30, 60];

    private const int STABILITY_SECONDS = 60;

    private int $consecutiveFailures = 0;

    private float $nextRetryAt = 0;

    private ?float $stableSince = null;

    public function mayRetry(float $now): bool
    {
        return $now >= $this->nextRetryAt;
    }

    public function recordStart(float $now): void
    {
        if ($this->consecutiveFailures > 0) {
            $this->stableSince ??= $now;
        }
    }

    public function recordFailure(float $now): void
    {
        $this->consecutiveFailures++;
        $this->nextRetryAt = $now + self::DELAYS[
            min($this->consecutiveFailures - 1, count(self::DELAYS) - 1)
        ];
        $this->stableSince = null;
    }

    public function resetWhenStable(float $now): bool
    {
        if ($this->consecutiveFailures === 0
            || $this->stableSince === null
            || $now - $this->stableSince < self::STABILITY_SECONDS) {
            return false;
        }

        $this->consecutiveFailures = 0;
        $this->stableSince = null;

        return true;
    }
}
