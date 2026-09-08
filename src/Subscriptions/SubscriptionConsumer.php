<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Illuminate\Cache\RateLimiter;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\DatabaseQueue;
use Illuminate\Support\Facades\Log;
use JsonException;
use LogicException;
use PDO;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\CanWaitForConsumerIo;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageEnvelopeException;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\TransportContext;
use Throwable;

class SubscriptionConsumer
{
    private bool $stopping = false;

    private int $nextLaneIndex = 0;

    public function __construct(
        private SpoolrailManager $manager,
        private SubscriptionRegistry $subscriptions,
        private MessageEnvelope $envelope,
        private QueueFactory $queues,
        private QueueHandoff $queueHandoff,
        private ConsumerConfig $config,
        private ExceptionHandler $exceptions,
        private RateLimiter $limiter,
        private Repository $configuration,
    ) {}

    /**
     * @param  non-empty-list<string>  $subscriptionNames
     */
    public function consume(array $subscriptionNames, bool $stopWhenIdle = false): void
    {
        $idleWaitMilliseconds = $this->config->idleWaitMilliseconds();
        [$driver, $connectionName, $driverName, $lanes] = $this->resolve($subscriptionNames);

        for (; ;) {
            $now = $this->now();
            $this->logRecoveries($lanes, $now);
            $this->startReceives($driver, $lanes, $now);
            $handoffAttempted = $this->handoffOne(
                $driver,
                $driverName,
                $connectionName,
                $lanes,
            );
            $releasesStarted = $this->startReleases($driver, $lanes);

            if ($this->shouldStop($lanes, $stopWhenIdle)) {
                $this->closeDriver($driver);

                return;
            }
            if ($handoffAttempted) {
                continue;
            }
            if ($releasesStarted) {
                continue;
            }

            $this->waitForProgress($driver, $idleWaitMilliseconds);
        }
    }

    public function stop(): void
    {
        $this->stopping = true;
    }

    public function ensureCanConsume(string $subscriptionName): void
    {
        $this->resolve([$subscriptionName]);
    }

    /**
     * @param  non-empty-list<string>  $subscriptionNames
     * @return array{Driver<covariant mixed>, string, string, non-empty-list<SubscriptionLane>}
     */
    private function resolve(array $subscriptionNames): array
    {
        $defaultConnection = $this->manager->defaultConnectionName();
        $connectionName = null;
        $lanes = [];

        foreach ($subscriptionNames as $subscriptionName) {
            $subscription = $this->subscriptions->findOrFail($subscriptionName);
            $laneConnection = $subscription->connectionName($defaultConnection);
            $connectionName ??= $laneConnection;

            if ($laneConnection !== $connectionName) {
                throw new LogicException('A consumer process cannot mix Spoolrail connections.');
            }

            $queue = $this->queues->connection($subscription->queueConnectionName());
            $this->rejectTransactionalDatabaseQueue($queue);
            $lanes[] = new SubscriptionLane($subscription, $queue);
        }

        $this->queueHandoff->ensureConfigured();

        return [
            $this->manager->connection($connectionName)->consumerDriver(),
            $connectionName,
            $this->manager->driverName($connectionName),
            $lanes,
        ];
    }

    /**
     * @param  Driver<covariant mixed>  $driver
     * @param  list<SubscriptionLane>  $lanes
     */
    private function startReceives(Driver $driver, array $lanes, float $now): void
    {
        if ($this->stopping) {
            return;
        }

        foreach ($lanes as $lane) {
            if (! $lane->mayReceive($now)) {
                continue;
            }

            $lane->receiveStarted($now);
            $driver->receive(
                $lane->subscription->name(),
                function (array $deliveries) use ($lane): void {
                    $this->receiveCompleted($lane, $deliveries);
                },
                function (Throwable $exception) use ($lane): void {
                    $this->receiveFailed($lane, $exception);
                },
            );
        }
    }

    /**
     * @param  list<Delivery<mixed>>  $deliveries
     */
    private function receiveCompleted(
        SubscriptionLane $lane,
        array $deliveries,
    ): void {
        if ($this->stopping) {
            $lane->receivedAfterShutdown($deliveries);
        } else {
            $lane->received($deliveries);
        }
    }

    private function receiveFailed(
        SubscriptionLane $lane,
        Throwable $exception,
    ): void {
        if ($this->stopping) {
            $lane->received([]);

            return;
        }

        $lane->receiveFailed($this->now());
        $this->report($lane, $exception);
    }

    /**
     * @param  Driver<covariant mixed>  $driver
     * @param  list<SubscriptionLane>  $lanes
     */
    private function handoffOne(
        Driver $driver,
        string $driverName,
        string $connectionName,
        array $lanes,
    ): bool {
        $lane = $this->nextReadyLane($lanes);

        if (! $lane instanceof SubscriptionLane) {
            return false;
        }

        $delivery = $lane->activateNextDelivery();

        try {
            $discarded = $this->handoff($delivery, $driverName, $connectionName, $lane);
        } catch (Throwable $exception) {
            $lane->failedBeforeAcknowledgment($this->now());
            $this->report($lane, $exception);

            return true;
        }

        $driver->acknowledge(
            $delivery,
            function () use ($lane, $delivery, $discarded): void {
                $lane->acknowledged();

                if ($discarded instanceof InvalidMessageEnvelopeException) {
                    $this->reportDiscard($lane, $delivery, $discarded);
                }
            },
            function (Throwable $exception) use ($lane): void {
                $lane->acknowledgmentFailed($this->now());
                $this->report($lane, $exception);
            },
        );

        return true;
    }

    /** @param list<SubscriptionLane> $lanes */
    private function nextReadyLane(array $lanes): ?SubscriptionLane
    {
        $count = count($lanes);

        for ($offset = 0; $offset < $count; $offset++) {
            $index = ($this->nextLaneIndex + $offset) % $count;
            $lane = $lanes[$index];

            if ($lane->hasReadyDelivery()) {
                $this->nextLaneIndex = ($index + 1) % $count;

                return $lane;
            }
        }

        return null;
    }

    /**
     * @param  Driver<covariant mixed>  $driver
     * @param  list<SubscriptionLane>  $lanes
     */
    private function startReleases(Driver $driver, array $lanes): bool
    {
        $progressed = false;

        foreach ($lanes as $lane) {
            if (! $lane->hasDeliveryToRelease()) {
                continue;
            }

            $progressed = true;
            $driver->release(
                $lane->beginRelease(),
                fn () => $lane->finishRelease(),
                function (Throwable $exception) use ($lane): void {
                    $lane->finishRelease();
                    $this->report($lane, $exception);
                },
            );
        }

        return $progressed;
    }

    /** @param  Delivery<mixed>  $delivery */
    private function handoff(
        Delivery $delivery,
        string $driverName,
        string $connectionName,
        SubscriptionLane $lane,
    ): ?InvalidMessageEnvelopeException {
        $subscription = $lane->subscription;
        $transport = new TransportContext(
            driver: $driverName,
            connectionName: $connectionName,
            topic: $subscription->topic(),
            subscription: $subscription->name(),
            headers: $delivery->headers,
            transportMessageId: $delivery->transportMessageId,
            transportPublishedAt: $delivery->transportPublishedAt,
            redelivered: $delivery->redelivered,
            orderingKey: $delivery->orderingKey,
        );

        try {
            $message = $this->envelope->decode($delivery->body);
        } catch (InvalidMessageEnvelopeException $exception) {
            return $this->discardInvalidMessage($delivery->body, $transport, $exception);
        }

        $this->queueHandoff->push($subscription, $message->withTransport($transport), $lane->queue);

        return null;
    }

    private function discardInvalidMessage(
        string $body,
        TransportContext $transport,
        InvalidMessageEnvelopeException $exception,
    ): ?InvalidMessageEnvelopeException {
        try {
            $envelope = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $jsonException) {
            if (in_array($jsonException->getCode(), [
                JSON_ERROR_STATE_MISMATCH,
                JSON_ERROR_CTRL_CHAR,
                JSON_ERROR_SYNTAX,
                JSON_ERROR_UTF8,
                JSON_ERROR_UTF16,
            ], true)) {
                return $exception;
            }

            throw $exception;
        }

        if (! is_array($envelope)) {
            return $exception;
        }

        if (! $this->manager->shouldDiscardInvalidMessage($envelope, $transport)) {
            throw $exception;
        }

        return null;
    }

    /** @param Delivery<mixed> $delivery */
    private function reportDiscard(
        SubscriptionLane $lane,
        Delivery $delivery,
        InvalidMessageEnvelopeException $exception,
    ): void {
        if (! $this->shouldReportDiscard($lane, $exception)) {
            return;
        }

        try {
            Log::error('Spoolrail discarded an invalid message classified as non-retryable.', [
                'subscription' => $lane->subscription->name(),
                'transport_message_id' => $delivery->transportMessageId,
                'body_fingerprint' => hash('sha256', $delivery->body),
                'reason' => $exception->getMessage(),
            ]);
        } catch (Throwable) {
            // Reporting must not interrupt consumption after settlement.
        }
    }

    private function shouldReportDiscard(
        SubscriptionLane $lane,
        InvalidMessageEnvelopeException $exception,
    ): bool {
        $cooldown = $this->configuration->get('spoolrail.consumer.exception_cooldown', 300);

        if (! is_int($cooldown) || $cooldown < 1) {
            return true;
        }

        $subscription = $lane->subscription->name();
        $reason = $exception->getMessage();
        $key = "spoolrail:consumer:discard:$subscription:".hash('sha256', $reason);

        try {
            return $this->limiter->attempt(
                $key,
                1,
                static fn (): bool => true,
                $cooldown,
            ) !== false;
        } catch (Throwable) {
            return true;
        }
    }

    private function report(SubscriptionLane $lane, Throwable $exception): void
    {
        try {
            $this->exceptions->report(ConsumerException::subscriptionFailed(
                $lane->subscription->name(),
                $exception,
            ));
        } catch (Throwable) {
            // Reporting must not interrupt healthy sibling lanes.
        }
    }

    private function logRecovery(string $subscription): void
    {
        try {
            Log::notice('Spoolrail subscription recovered.', ['subscription' => $subscription]);
        } catch (Throwable) {
            // Recovery reporting must not interrupt consumption.
        }
    }

    /** @param  list<SubscriptionLane>  $lanes */
    private function logRecoveries(array $lanes, float $now): void
    {
        foreach ($lanes as $lane) {
            if ($lane->resetBackoffWhenStable($now)) {
                $this->logRecovery($lane->subscription->name());
            }
        }
    }

    /** @param  list<SubscriptionLane>  $lanes */
    private function shouldStop(array $lanes, bool $stopWhenIdle): bool
    {
        if ($this->stopping) {
            return ! array_any(
                $lanes,
                static fn (SubscriptionLane $lane): bool => $lane->hasWorkToDrain(),
            );
        }

        return $stopWhenIdle && array_all(
            $lanes,
            static fn (SubscriptionLane $lane): bool => $lane->isIdle(),
        );
    }

    /** @param  Driver<covariant mixed>  $driver */
    private function waitForProgress(Driver $driver, int $idleWaitMilliseconds): void
    {
        if ($driver instanceof CanWaitForConsumerIo) {
            $driver->waitForConsumerIo();

            return;
        }

        usleep($idleWaitMilliseconds * 1_000);
    }

    /** @param  Driver<covariant mixed>  $driver */
    private function closeDriver(Driver $driver): void
    {
        if ($driver instanceof CanClose) {
            $driver->close();
        }
    }

    private function rejectTransactionalDatabaseQueue(Queue $queue): void
    {
        if (! $queue instanceof DatabaseQueue || ! $this->hasOpenTransaction($queue)) {
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

    protected function now(): float
    {
        return microtime(true);
    }
}
