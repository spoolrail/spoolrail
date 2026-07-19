<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Closure;
use Spoolrail\Spoolrail\Contracts\ConsumableDriver;
use Spoolrail\Spoolrail\Contracts\Delivery;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

class ArrayDriver implements ConsumableDriver
{
    /** @var array<string, list<string>> */
    private array $deliveries = [];

    public function __construct(
        private readonly string $connectionName,
        private readonly string $defaultConnection,
        private readonly SubscriptionRegistry $subscriptions,
    ) {}

    public function publish(string $topic, string $body): void
    {
        foreach ($this->subscriptions->forTopic(
            $topic,
            $this->connectionName,
            $this->defaultConnection,
        ) as $subscription) {
            $this->deliveries[$subscription->name()][] = $body;
        }
    }

    /**
     * @param  Closure(Delivery): void  $handle
     */
    public function consume(string $subscription, Closure $handle): void
    {
        while ($delivery = $this->nextDelivery($subscription)) {
            try {
                $handle($delivery);
            } finally {
                if (! $delivery->isAcknowledged()) {
                    $this->release($subscription, $delivery);
                }
            }

            if (! $delivery->isAcknowledged()) {
                return;
            }
        }
    }

    private function nextDelivery(string $subscription): ?ArrayDelivery
    {
        if (($this->deliveries[$subscription] ?? []) === []) {
            return null;
        }

        $body = array_shift($this->deliveries[$subscription]);

        return new ArrayDelivery($body);
    }

    private function release(string $subscription, ArrayDelivery $delivery): void
    {
        array_unshift($this->deliveries[$subscription], $delivery->body());
    }
}
