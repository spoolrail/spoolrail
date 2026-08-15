<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\PubSub;

use Closure;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\PubSubTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Throwable;

class PendingTopology implements TopologyPlan
{
    /** @var list<Topic> */
    private array $topics = [];

    /** @var list<array{subscription: Subscription, message_ordering: bool, exactly_once: bool}> */
    private array $subscriptions = [];

    /** @var list<array{subscription: Subscription, exactly_once: bool}> */
    private array $exactlyOnceUpdates = [];

    public function apply(): void
    {
        $this->createTopics();
        $this->createSubscriptions();
        $this->updateExactlyOnceDelivery();
    }

    public function addTopic(Topic $topic): void
    {
        $this->topics[] = $topic;
    }

    public function addSubscription(
        Subscription $subscription,
        bool $messageOrdering,
        bool $exactlyOnce,
    ): void {
        $this->subscriptions[] = [
            'subscription' => $subscription,
            'message_ordering' => $messageOrdering,
            'exactly_once' => $exactlyOnce,
        ];
    }

    public function updateExactlyOnce(Subscription $subscription, bool $enabled): void
    {
        $this->exactlyOnceUpdates[] = [
            'subscription' => $subscription,
            'exactly_once' => $enabled,
        ];
    }

    private function createTopics(): void
    {
        foreach ($this->topics as $topic) {
            $this->applyRequest(
                "creating topic [{$topic->name()}]",
                static fn (): mixed => $topic->create(),
            );
        }
    }

    private function createSubscriptions(): void
    {
        foreach ($this->subscriptions as $pending) {
            $subscription = $pending['subscription'];

            $this->applyRequest(
                "creating subscription [{$subscription->name()}]",
                static fn (): mixed => $subscription->create([
                    'enableMessageOrdering' => $pending['message_ordering'],
                    'enableExactlyOnceDelivery' => $pending['exactly_once'],
                ]),
            );
        }
    }

    private function updateExactlyOnceDelivery(): void
    {
        foreach ($this->exactlyOnceUpdates as $pending) {
            $subscription = $pending['subscription'];

            $this->applyRequest(
                "updating subscription [{$subscription->name()}]",
                static fn (): mixed => $subscription->update(
                    ['enableExactlyOnceDelivery' => $pending['exactly_once']],
                    ['updateMask' => ['enableExactlyOnceDelivery']],
                ),
            );
        }
    }

    /**
     * @template TResult
     *
     * @param  Closure(): TResult  $request
     * @return TResult
     */
    private function applyRequest(string $operation, Closure $request): mixed
    {
        try {
            return $request();
        } catch (ServiceException $exception) {
            $failure = PubSubTopologyException::operationFailed($operation, $exception);

            if ($failure->shouldRetry()) {
                throw TopologySyncRequiresRetryException::afterFailure($failure);
            }

            throw $failure;
        } catch (Throwable $exception) {
            throw PubSubTopologyException::operationFailed($operation, $exception);
        }
    }
}
