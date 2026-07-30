<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\DatabaseQueue;
use PDO;
use Spoolrail\Spoolrail\Exceptions\DatabaseQueueTransactionException;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Jobs\HandlerQueuePolicy;
use Spoolrail\Spoolrail\MessageSerializer;
use Spoolrail\Spoolrail\SpoolrailManager;

readonly class SubscriptionConsumer
{
    public function __construct(
        private SpoolrailManager $manager,
        private SubscriptionRegistry $subscriptions,
        private MessageSerializer $serializer,
        private QueueFactory $queues,
        private HandlerQueuePolicy $handlerQueuePolicy,
    ) {}

    public function consume(string $subscriptionName): void
    {
        $subscription = $this->subscriptions->active($subscriptionName);
        $connection = $this->manager->connection(
            $subscription->connectionName($this->manager->defaultConnectionName()),
        );

        $queue = $this->queues->connection($subscription->queueConnectionName());

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
        $job = new HandleMessageJob($message, $subscription->name());

        $this->handlerQueuePolicy->capture($subscription->handlerClass(), $message, $job);

        $queue->push(
            $job,
            '',
            $subscription->queueName(),
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
