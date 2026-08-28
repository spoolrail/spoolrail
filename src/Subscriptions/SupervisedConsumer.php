<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

/** @internal */
class SupervisedConsumer
{
    private ?ConsumerProcess $process = null;

    private RecoveryBackoff $recovery;

    /** @param  non-empty-list<string>  $subscriptionNames */
    public function __construct(public readonly array $subscriptionNames)
    {
        $this->recovery = new RecoveryBackoff;
    }

    public function process(): ?ConsumerProcess
    {
        return $this->process;
    }

    public function markAsStarted(ConsumerProcess $process, float $now): void
    {
        $this->process = $process;
        $this->recovery->recordStart($now);
    }

    public function markAsFailed(float $now): void
    {
        $this->process = null;
        $this->recovery->recordFailure($now);
    }

    public function isReadyToStart(float $now): bool
    {
        return ! $this->process instanceof ConsumerProcess && $this->recovery->mayRetry($now);
    }

    public function resetBackoffWhenStable(float $now): void
    {
        $this->recovery->resetWhenStable($now);
    }
}
