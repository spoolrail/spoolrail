<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Closure;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\TransportContext;
use Throwable;

class ArrayDriver implements Driver
{
    /**
     * @var array<string, list<array{
     *     topic: string,
     *     body: string,
     *     headers: array<string, string>,
     *     redelivered: bool
     * }>>
     */
    private array $deliveries = [];

    public function __construct(
        private readonly string $connectionName,
        private readonly string $defaultConnectionName,
        private readonly SubscriptionRegistry $subscriptions,
    ) {}

    /**
     * @param  array<string, string>  $headers
     */
    public function publish(string $topic, string $body, array $headers): void
    {
        foreach ($this->matchingSubscriptions($topic) as $subscription) {
            $this->deliveries[$subscription->name()][] = [
                'topic' => $topic,
                'body' => $body,
                'headers' => $headers,
                'redelivered' => false,
            ];
        }
    }

    /**
     * @param  Closure(string, TransportContext): void  $handoff
     *
     * @throws Throwable
     */
    public function consume(string $subscription, Closure $handoff): void
    {
        while (($delivery = $this->reserveNextDelivery($subscription)) !== null) {
            try {
                $handoff(
                    $delivery['body'],
                    new TransportContext(
                        driver: 'array',
                        connectionName: $this->connectionName,
                        topic: $delivery['topic'],
                        subscription: $subscription,
                        headers: $delivery['headers'],
                        redelivered: $delivery['redelivered'],
                    ),
                );
            } catch (Throwable $exception) {
                $this->release($subscription, $delivery);

                throw $exception;
            }
        }
    }

    /**
     * @return array{
     *     topic: string,
     *     body: string,
     *     headers: array<string, string>,
     *     redelivered: bool
     * }|null
     */
    private function reserveNextDelivery(string $subscription): ?array
    {
        if (($this->deliveries[$subscription] ?? []) === []) {
            return null;
        }

        return array_shift($this->deliveries[$subscription]);
    }

    /**
     * @param  array{
     *     topic: string,
     *     body: string,
     *     headers: array<string, string>,
     *     redelivered: bool
     * }  $delivery
     */
    private function release(string $subscription, array $delivery): void
    {
        $delivery['redelivered'] = true;

        array_unshift($this->deliveries[$subscription], $delivery);
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
