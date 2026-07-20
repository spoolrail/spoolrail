<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\DatabaseQueue;
use PDO;
use Spoolrail\Spoolrail\Exceptions\DatabaseQueueTransactionException;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\MessageSerializer;
use Spoolrail\Spoolrail\SpoolrailManager;

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
        $connection = $this->manager->connection(
            $subscription->connection($this->manager->getDefaultConnection()),
        );

        $queue = $this->queues->connection($subscription->queueConnection());

        $this->rejectTransactionalDatabaseQueue($queue);

        $connection->consume(
            $subscription->name(),
            function (string $body) use ($subscription, $queue): void {
                $this->handoff($body, $subscription, $queue);
            },
        );
    }

    private function handoff(string $body, Subscription $subscription, Queue $queue): void
    {
        $message = $this->serializer->deserialize($body);

        $queue->push(
            new HandleMessageJob($message, $subscription->name()),
            '',
            $subscription->queue(),
        );
    }

    private function rejectTransactionalDatabaseQueue(Queue $queue): void
    {
        if (! $queue instanceof DatabaseQueue) {
            return;
        }

        $database = $queue->getDatabase();
        $pdo = $database->getRawPdo();

        if ($database->transactionLevel() === 0 && (! $pdo instanceof PDO || ! $pdo->inTransaction())) {
            return;
        }

        throw new DatabaseQueueTransactionException;
    }
}
