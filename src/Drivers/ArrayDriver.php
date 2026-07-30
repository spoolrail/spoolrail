<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Closure;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Throwable;

class ArrayDriver implements Driver
{
    /** @var array<string, list<string>> */
    private array $deliveries = [];

    public function __construct(
        private readonly string $connectionName,
        private readonly string $defaultConnectionName,
        private readonly SubscriptionRegistry $subscriptions,
    ) {}

    public function publish(string $topic, string $body): void
    {
        foreach ($this->matchingSubscriptions($topic) as $subscription) {
            $this->deliveries[$subscription->name()][] = $body;
        }
    }

    /**
     * @param  Closure(string): void  $handoff
     *
     * @throws Throwable
     */
    public function consume(string $subscription, Closure $handoff): void
    {
        while (($body = $this->nextDelivery($subscription)) !== null) {
            try {
                $handoff($body);
            } catch (Throwable $exception) {
                $this->release($subscription, $body);

                throw $exception;
            }
        }
    }

    private function nextDelivery(string $subscription): ?string
    {
        if (($this->deliveries[$subscription] ?? []) === []) {
            return null;
        }

        return array_shift($this->deliveries[$subscription]);
    }

    private function release(string $subscription, string $body): void
    {
        array_unshift($this->deliveries[$subscription], $body);
    }

    /**
     * @return list<Subscription>
     */
    private function matchingSubscriptions(string $topic): array
    {
        return array_values(array_filter(
            $this->subscriptions->all(),
            fn (Subscription $subscription): bool => $subscription->topic() === $topic
                && $subscription->connectionName($this->defaultConnectionName) === $this->connectionName,
        ));
    }
}
