<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\DatabaseQueue;
use LogicException;
use PDO;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\TransportContext;

readonly class SubscriptionConsumer
{
    public function __construct(
        private SpoolrailManager $manager,
        private SubscriptionRegistry $subscriptions,
        private MessageEnvelope $envelope,
        private QueueFactory $queues,
        private QueueHandoff $queueHandoff,
    ) {}

    public function consume(string $subscriptionName): void
    {
        $subscription = $this->subscriptions->findOrFail($subscriptionName);
        $connection = $this->manager->connection(
            $subscription->connectionName($this->manager->defaultConnectionName()),
        );

        $queue = $this->queues->connection($subscription->queueConnectionName());

        $this->rejectTransactionalDatabaseQueue($queue);
        $this->queueHandoff->ensureConfigured();

        $connection->consume(
            $subscription->name(),
            function (string $body, TransportContext $transport) use ($subscription, $queue): void {
                $this->handoff($body, $transport, $subscription, $queue);
            },
        );
    }

    private function handoff(
        string $body,
        TransportContext $transport,
        Subscription $subscription,
        Queue $queue,
    ): void {
        $message = $this->envelope->decode($body)->withTransport($transport);

        $this->queueHandoff->push($subscription, $message, $queue);
    }

    private function rejectTransactionalDatabaseQueue(Queue $queue): void
    {
        if (! $queue instanceof DatabaseQueue) {
            return;
        }

        if (! $this->hasOpenTransaction($queue)) {
            return;
        }

        throw new LogicException(
            "Laravel's database Queue cannot accept a Spoolrail handoff while its connection has an open transaction. Commit or roll back that transaction before consuming, or use another Queue connection.",
        );
    }

    private function hasOpenTransaction(DatabaseQueue $queue): bool
    {
        $database = $queue->getDatabase();
        $pdo = $database->getRawPdo();

        if ($database->transactionLevel() > 0) {
            return true;
        }

        return $pdo instanceof PDO && $pdo->inTransaction();
    }
}
