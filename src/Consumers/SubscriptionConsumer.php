<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Consumers;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Spoolrail\Spoolrail\ConsumableConnection;
use Spoolrail\Spoolrail\Contracts\Delivery;
use Spoolrail\Spoolrail\Exceptions\ConnectionNotConsumableException;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Serialization\MessageSerializer;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

class SubscriptionConsumer
{
    public function __construct(
        private readonly SpoolrailManager $manager,
        private readonly SubscriptionRegistry $subscriptions,
        private readonly MessageSerializer $serializer,
        private readonly QueueFactory $queues,
    ) {}

    public function consume(string $subscriptionName): void
    {
        $subscription = $this->subscriptions->get($subscriptionName);
        $connectionName = $subscription->connection($this->manager->getDefaultConnection());
        $connection = $this->manager->connection($connectionName);

        if (! $connection instanceof ConsumableConnection) {
            throw new ConnectionNotConsumableException($connectionName);
        }

        $connection->consume(
            $subscription->name(),
            function (Delivery $delivery) use ($subscription): void {
                $this->handoff($delivery, $subscription);
            },
        );
    }

    private function handoff(Delivery $delivery, Subscription $subscription): void
    {
        $message = $this->serializer->deserialize($delivery->body());

        $this->queues
            ->connection($subscription->queueConnection())
            ->push(
                new HandleMessageJob($message, $subscription->name()),
                '',
                $subscription->queue(),
            );

        $delivery->acknowledge();
    }
}
