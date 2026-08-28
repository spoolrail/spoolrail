<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Illuminate\Contracts\Queue\Queue;
use Spoolrail\Spoolrail\Delivery;

/** @internal */
class SubscriptionLane
{
    /** @var list<Delivery<mixed>> */
    private array $buffer = [];

    /** @var list<Delivery<mixed>> */
    private array $releases = [];

    /** @var Delivery<mixed>|null */
    private ?Delivery $active = null;

    private bool $receiving = false;

    private bool $releasing = false;

    private bool $lastReceiveWasEmpty = false;

    private RecoveryBackoff $recovery;

    public function __construct(
        public readonly Subscription $subscription,
        public readonly Queue $queue,
    ) {
        $this->recovery = new RecoveryBackoff;
    }

    public function mayReceive(float $now): bool
    {
        return ! $this->receiving
            && ! $this->hasWorkToDrain()
            && $this->recovery->mayRetry($now);
    }

    public function receiveStarted(float $now): void
    {
        $this->receiving = true;
        $this->lastReceiveWasEmpty = false;
        $this->recovery->recordStart($now);
    }

    /** @param  list<Delivery<mixed>>  $deliveries */
    public function received(array $deliveries): void
    {
        $this->receiving = false;
        $this->lastReceiveWasEmpty = $deliveries === [];
        array_push($this->buffer, ...$deliveries);
    }

    public function receiveFailed(float $now): void
    {
        $this->receiving = false;
        $this->fail($now);
    }

    public function hasReadyDelivery(): bool
    {
        return ! $this->active instanceof Delivery && $this->buffer !== [];
    }

    /** @return Delivery<mixed> */
    public function activateNextDelivery(): Delivery
    {
        /** @var Delivery<mixed> $delivery */
        $delivery = array_shift($this->buffer);

        return $this->active = $delivery;
    }

    public function acknowledged(): void
    {
        $this->active = null;
    }

    public function failedBeforeAcknowledgment(float $now): void
    {
        if ($this->active instanceof Delivery) {
            $this->releases[] = $this->active;
        }

        $this->fail($now);
    }

    public function acknowledgmentFailed(float $now): void
    {
        $this->active = null;
        $this->fail($now);
    }

    /** @param  list<Delivery<mixed>>  $deliveries */
    public function receivedAfterShutdown(array $deliveries): void
    {
        $this->receiving = false;
        array_push($this->releases, ...$deliveries);
    }

    public function hasDeliveryToRelease(): bool
    {
        return ! $this->releasing && $this->releases !== [];
    }

    /** @return Delivery<mixed> */
    public function beginRelease(): Delivery
    {
        $this->releasing = true;

        /** @var Delivery<mixed> */
        return array_shift($this->releases);
    }

    public function finishRelease(): void
    {
        $this->releasing = false;
    }

    public function isIdle(): bool
    {
        return $this->lastReceiveWasEmpty && ! $this->hasWorkToDrain();
    }

    public function hasWorkToDrain(): bool
    {
        return $this->active instanceof Delivery
            || $this->buffer !== []
            || $this->releases !== []
            || $this->releasing;
    }

    public function resetBackoffWhenStable(float $now): bool
    {
        return $this->recovery->resetWhenStable($now);
    }

    private function fail(float $now): void
    {
        $this->recovery->recordFailure($now);
        $this->lastReceiveWasEmpty = false;
        $this->active = null;
        array_push($this->releases, ...$this->buffer);
        $this->buffer = [];
    }
}
