<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\PubSub;

use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\PubSubTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Throwable;

class Topology implements CanManageTopology
{
    public function __construct(
        private ConnectionConfig $config,
        private PubSubClient $client,
    ) {}

    /**
     * @param  list<Subscription>  $subscriptions
     */
    public function planSync(array $subscriptions, string $ownershipPrefix): TopologyPlan
    {
        try {
            return $this->buildPlan($subscriptions, $ownershipPrefix);
        } catch (PubSubTopologyException $exception) {
            throw $exception;
        } catch (ServiceException $exception) {
            $failure = PubSubTopologyException::operationFailed('planning synchronization', $exception);

            if ($failure->shouldRetry()) {
                throw TopologySyncRequiresRetryException::afterFailure($failure);
            }

            throw $failure;
        } catch (Throwable $exception) {
            throw PubSubTopologyException::operationFailed('planning synchronization', $exception);
        }
    }

    /**
     * @param  list<Subscription>  $subscriptions
     * @return list<string>
     */
    public function undeclaredSubscriptionResourceNames(
        array $subscriptions,
        string $ownershipPrefix,
    ): array {
        try {
            $declared = array_map(
                static fn (Subscription $subscription): string => ResourceName::subscription(
                    $ownershipPrefix,
                    $subscription->name(),
                ),
                $subscriptions,
            );
            $owned = $this->ownedSubscriptionResourceNames($ownershipPrefix);
            $undeclared = array_values(array_diff($owned, $declared));

            sort($undeclared);

            return $undeclared;
        } catch (PubSubTopologyException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw PubSubTopologyException::operationFailed(
                'listing application-owned subscriptions',
                $exception,
            );
        }
    }

    public function deleteSubscription(string $physicalName): void
    {
        try {
            $this->client->subscription($physicalName)->delete();
        } catch (Throwable $exception) {
            throw PubSubTopologyException::operationFailed(
                "deleting subscription [$physicalName]",
                $exception,
            );
        }
    }

    public function deleteTopic(string $topic): void
    {
        $physicalTopic = ResourceName::topic($topic);
        $nativeTopic = $this->client->topic($physicalTopic);

        try {
            $nativeTopic->info();
        } catch (NotFoundException) {
            throw PubSubTopologyException::topicMissing($physicalTopic);
        } catch (Throwable $exception) {
            throw PubSubTopologyException::operationFailed(
                "reading topic [$physicalTopic]",
                $exception,
            );
        }

        try {
            foreach ($nativeTopic->subscriptions(['resultLimit' => 1]) as $subscription) {
                throw PubSubTopologyException::topicHasSubscriptions($physicalTopic);
            }

            $nativeTopic->delete();
        } catch (PubSubTopologyException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw PubSubTopologyException::operationFailed(
                "deleting topic [$physicalTopic]",
                $exception,
            );
        }
    }

    /**
     * @param  list<Subscription>  $subscriptions
     */
    private function buildPlan(array $subscriptions, string $ownershipPrefix): PendingTopology
    {
        $plan = new PendingTopology;
        $topics = [];

        foreach ($subscriptions as $subscription) {
            $topicName = ResourceName::topic($subscription->topic());

            if (! isset($topics[$topicName])) {
                $topics[$topicName] = $this->planTopic($plan, $topicName);
            }

            $this->planSubscription(
                $plan,
                $subscription,
                $ownershipPrefix,
                $topicName,
            );
        }

        return $plan;
    }

    private function planTopic(PendingTopology $plan, string $topicName): Topic
    {
        $topic = $this->client->topic($topicName);

        try {
            $topic->info();
        } catch (NotFoundException) {
            $plan->addTopic($topic);
        }

        return $topic;
    }

    private function planSubscription(
        PendingTopology $plan,
        Subscription $subscription,
        string $ownershipPrefix,
        string $topicName,
    ): void {
        $physicalName = ResourceName::subscription(
            $ownershipPrefix,
            $subscription->name(),
        );
        $native = $this->client->subscription($physicalName, $topicName);

        try {
            $info = $native->info();
        } catch (NotFoundException) {
            $plan->addSubscription(
                $native,
                $this->config->messageOrdering(),
                $this->config->exactlyOnce(),
            );

            return;
        }

        $this->ensureSubscriptionIsCompatible($physicalName, $topicName, $info);

        $exactlyOnce = ($info['enableExactlyOnceDelivery'] ?? false) === true;

        if ($exactlyOnce !== $this->config->exactlyOnce()) {
            $plan->updateExactlyOnce($native, $this->config->exactlyOnce());
        }
    }

    /**
     * @param  array<array-key, mixed>  $info
     */
    private function ensureSubscriptionIsCompatible(
        string $subscription,
        string $topic,
        array $info,
    ): void {
        $expectedTopic = ResourceName::topicPath($this->config->projectId(), $topic);

        if (($info['topic'] ?? null) !== $expectedTopic) {
            throw PubSubTopologyException::incompatibleSubscription(
                $subscription,
                "it must belong to topic [$expectedTopic]",
            );
        }

        if ($this->hasNonPullDelivery($info)) {
            throw PubSubTopologyException::incompatibleSubscription(
                $subscription,
                'it must use pull delivery',
            );
        }

        $messageOrdering = ($info['enableMessageOrdering'] ?? false) === true;

        if ($messageOrdering !== $this->config->messageOrdering()) {
            throw PubSubTopologyException::immutableOrdering(
                $subscription,
                $messageOrdering,
                $this->config->messageOrdering(),
            );
        }
    }

    /**
     * @param  array<array-key, mixed>  $info
     */
    private function hasNonPullDelivery(array $info): bool
    {
        $pushConfig = $info['pushConfig'] ?? null;

        if (is_array($pushConfig) && ($pushConfig['pushEndpoint'] ?? '') !== '') {
            return true;
        }

        foreach (['bigQueryConfig', 'cloudStorageConfig', 'bigtableConfig'] as $config) {
            if (isset($info[$config]) && is_array($info[$config]) && $info[$config] !== []) {
                return true;
            }
        }

        return ($info['detached'] ?? false) === true;
    }

    /**
     * @return list<string>
     */
    private function ownedSubscriptionResourceNames(string $ownershipPrefix): array
    {
        $owned = [];
        $namespace = "$ownershipPrefix-";

        foreach ($this->client->subscriptions() as $subscription) {
            $physicalName = ResourceName::subscriptionId((string) $subscription->name());

            if ($physicalName !== null && str_starts_with($physicalName, $namespace)) {
                $owned[] = $physicalName;
            }
        }

        return $owned;
    }
}
