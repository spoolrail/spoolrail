<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\SnsSqs;

use Aws\Exception\AwsException;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use Aws\Sts\StsClient;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\SnsSqsTopologyException;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Throwable;

class Topology implements CanManageTopology
{
    private const array FIFO_QUEUE_ATTRIBUTES = [
        'FifoQueue' => 'true',
        'DeduplicationScope' => 'messageGroup',
        'FifoThroughputLimit' => 'perMessageGroupId',
    ];

    private const array FIFO_TOPIC_ATTRIBUTES = [
        'FifoTopic' => 'true',
        'FifoThroughputScope' => 'MessageGroup',
    ];

    public function __construct(
        private ConnectionConfig $config,
        private SnsClient $sns,
        private SqsClient $sqs,
        private StsClient $sts,
        private QueuePolicy $queuePolicy,
    ) {}

    /**
     * @param  list<Subscription>  $subscriptions
     */
    public function planSync(array $subscriptions, string $ownershipPrefix): TopologyPlan
    {
        try {
            return $this->buildPlan($subscriptions, $ownershipPrefix);
        } catch (SnsSqsTopologyException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw SnsSqsTopologyException::operationFailed('planning synchronization', $exception);
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
            $queueNames = array_values(array_filter(
                array_keys($this->ownedQueueUrls($ownershipPrefix)),
                $this->matchesConfiguredQueueType(...),
            ));
            $declared = array_map(
                fn (Subscription $subscription): string => ResourceName::queue(
                    $ownershipPrefix,
                    $subscription->name(),
                    $this->config->fifo(),
                ),
                $subscriptions,
            );
            $undeclared = array_values(array_diff($queueNames, $declared));

            sort($undeclared);

            return $undeclared;
        } catch (SnsSqsTopologyException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw SnsSqsTopologyException::operationFailed('listing application-owned SQS queues', $exception);
        }
    }

    public function deleteSubscription(string $physicalName): void
    {
        try {
            $queueUrl = $this->queueUrl($physicalName);
            $attributes = $this->queueAttributes($queueUrl);
            $queueArn = $this->queueArn($physicalName, $attributes);
            $topicArn = $this->queuePolicy->sourceTopicArn(
                $physicalName,
                $attributes['Policy'] ?? null,
                $queueArn,
            );

            $this->unsubscribeQueue($topicArn, $queueArn);
            $this->sqs->deleteQueue(['QueueUrl' => $queueUrl]);
        } catch (SnsSqsTopologyException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw SnsSqsTopologyException::operationFailed("deleting SQS queue [$physicalName]", $exception);
        }
    }

    public function deleteTopic(string $topic): void
    {
        try {
            $physicalTopic = ResourceName::topic($topic, $this->config->fifo());
            $topicArn = ResourceName::topicArn($this->config, $topic);
            $attributes = $this->findTopicAttributesOrFail($physicalTopic, $topicArn);

            $this->ensureTopicIsCompatible($physicalTopic, $attributes);

            if ($this->subscriptionsFor($topicArn) !== []) {
                throw SnsSqsTopologyException::topicHasSubscriptions($physicalTopic);
            }

            $this->sns->deleteTopic(['TopicArn' => $topicArn]);
        } catch (SnsSqsTopologyException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw SnsSqsTopologyException::operationFailed("deleting SNS topic [$topic]", $exception);
        }
    }

    /**
     * @param  list<Subscription>  $subscriptions
     */
    private function buildPlan(array $subscriptions, string $ownershipPrefix): PendingTopology
    {
        $queueUrls = $this->ownedQueueUrls($ownershipPrefix);
        $plan = new PendingTopology($this->sns, $this->sqs);

        foreach ($subscriptions as $subscription) {
            $this->planSubscription($plan, $subscription, $ownershipPrefix, $queueUrls);
        }

        if ($plan->createsResources()) {
            $this->ensureResourcesWillBeCreatedInOwningAccount();
        }

        return $plan;
    }

    /**
     * @param  array<string, string>  $queueUrls
     */
    private function planSubscription(
        PendingTopology $plan,
        Subscription $subscription,
        string $ownershipPrefix,
        array $queueUrls,
    ): void {
        $topicName = ResourceName::topic($subscription->topic(), $this->config->fifo());
        $topicArn = ResourceName::topicArn($this->config, $subscription->topic());
        $queueName = ResourceName::queue(
            $ownershipPrefix,
            $subscription->name(),
            $this->config->fifo(),
        );
        $otherQueueName = ResourceName::queue(
            $ownershipPrefix,
            $subscription->name(),
            ! $this->config->fifo(),
        );
        $queueArn = ResourceName::queueArn($this->config, $queueName);

        $this->ensureOtherQueueTypeIsAbsent($queueUrls, $queueName, $otherQueueName);
        $this->planTopic($plan, $topicName, $topicArn);
        $this->planQueue($plan, $queueUrls[$queueName] ?? null, $queueName, $queueArn, $topicArn);
        $this->planRoute($plan, $topicArn, $queueName, $queueArn);
    }

    /**
     * @param  array<string, string>  $queueUrls
     */
    private function ensureOtherQueueTypeIsAbsent(
        array $queueUrls,
        string $expectedQueueName,
        string $otherQueueName,
    ): void {
        if (isset($queueUrls[$otherQueueName])) {
            throw SnsSqsTopologyException::conflictingQueueType(
                $expectedQueueName,
                $otherQueueName,
            );
        }
    }

    private function planTopic(
        PendingTopology $plan,
        string $topicName,
        string $topicArn,
    ): void {
        if ($plan->hasInspectedTopic($topicArn)) {
            return;
        }

        $attributes = $this->findTopicAttributes($topicArn);

        if ($attributes === null) {
            $plan->addTopic($topicName, $topicArn, $this->topicCreationAttributes());
            $plan->rememberTopicSubscriptions($topicArn, []);

            return;
        }

        $this->ensureTopicIsCompatible($topicName, $attributes);
        $plan->rememberTopicSubscriptions($topicArn, $this->subscriptionsFor($topicArn));
    }

    private function planQueue(
        PendingTopology $plan,
        ?string $queueUrl,
        string $queueName,
        string $queueArn,
        string $topicArn,
    ): void {
        if ($queueUrl === null) {
            $policy = $this->queuePolicy->withRoute($queueName, null, $queueArn, $topicArn);

            $plan->addQueue(
                $queueName,
                [
                    ...$this->queueCreationAttributes(),
                    'Policy' => $policy,
                ],
            );

            return;
        }

        $attributes = $this->queueAttributes($queueUrl);
        $this->ensureQueueArnMatches($queueName, $queueArn, $attributes);
        $this->ensureQueueIsCompatible($queueName, $attributes);
        $existingPolicy = $attributes['Policy'] ?? null;
        $updatedPolicy = $this->queuePolicy->withRoute(
            $queueName,
            $existingPolicy,
            $queueArn,
            $topicArn,
        );

        if ($updatedPolicy === $existingPolicy) {
            return;
        }

        $plan->addQueuePolicy($queueUrl, $updatedPolicy);
    }

    /**
     * @param  array<string, string>  $attributes
     */
    private function ensureQueueArnMatches(
        string $queueName,
        string $expectedArn,
        array $attributes,
    ): void {
        if ($this->queueArn($queueName, $attributes) !== $expectedArn) {
            throw SnsSqsTopologyException::incompatibleQueue(
                $queueName,
                "queue ARN must be [$expectedArn]",
            );
        }
    }

    private function planRoute(
        PendingTopology $plan,
        string $topicArn,
        string $queueName,
        string $queueArn,
    ): void {
        if ($this->hasRawSubscription($plan->subscriptionsFor($topicArn), $queueName, $queueArn)) {
            return;
        }

        $plan->addSubscription($topicArn, $queueArn);
    }

    /**
     * @return array<string, string>
     */
    private function topicCreationAttributes(): array
    {
        if (! $this->config->fifo()) {
            return [];
        }

        return self::FIFO_TOPIC_ATTRIBUTES;
    }

    /**
     * @return array<string, string>
     */
    private function queueCreationAttributes(): array
    {
        if (! $this->config->fifo()) {
            return [];
        }

        return self::FIFO_QUEUE_ATTRIBUTES;
    }

    /**
     * @param  array<string, string>  $attributes
     */
    private function ensureTopicIsCompatible(string $topic, array $attributes): void
    {
        if (! $this->config->fifo()) {
            return;
        }

        foreach (self::FIFO_TOPIC_ATTRIBUTES as $setting => $required) {
            if (($attributes[$setting] ?? null) !== $required) {
                throw SnsSqsTopologyException::incompatibleTopic(
                    $topic,
                    "[$setting] must be [$required]; Spoolrail does not mutate existing topics",
                );
            }
        }
    }

    /**
     * @param  array<string, string>  $attributes
     */
    private function ensureQueueIsCompatible(string $queue, array $attributes): void
    {
        if (! $this->config->fifo()) {
            return;
        }

        foreach (self::FIFO_QUEUE_ATTRIBUTES as $setting => $required) {
            if (($attributes[$setting] ?? null) !== $required) {
                throw SnsSqsTopologyException::incompatibleQueue(
                    $queue,
                    "[$setting] must be [$required]; Spoolrail does not mutate existing queues",
                );
            }
        }
    }

    private function ensureResourcesWillBeCreatedInOwningAccount(): void
    {
        $account = $this->sts->getCallerIdentity()->get('Account');

        if (! is_string($account) || $account !== $this->config->accountId()) {
            throw SnsSqsTopologyException::creationAccountMismatch(
                $this->config->accountId(),
                is_string($account) ? $account : 'unknown',
            );
        }
    }

    /**
     * @return array<string, string>
     */
    private function findTopicAttributesOrFail(string $topic, string $topicArn): array
    {
        $attributes = $this->findTopicAttributes($topicArn);

        if ($attributes === null) {
            throw SnsSqsTopologyException::topicMissing($topic);
        }

        return $attributes;
    }

    /**
     * @return array<string, string>|null
     */
    private function findTopicAttributes(string $topicArn): ?array
    {
        try {
            $attributes = $this->sns->getTopicAttributes([
                'TopicArn' => $topicArn,
            ])->get('Attributes');
        } catch (AwsException $exception) {
            if ($exception->getStatusCode() === 404 || str_contains((string) $exception->getAwsErrorCode(), 'NotFound')) {
                return null;
            }

            throw $exception;
        }

        return $this->stringMap($attributes);
    }

    /**
     * @return array<string, string>
     */
    private function queueAttributes(string $queueUrl): array
    {
        return $this->stringMap($this->sqs->getQueueAttributes([
            'QueueUrl' => $queueUrl,
            'AttributeNames' => ['All'],
        ])->get('Attributes'));
    }

    /**
     * @return array<string, string>
     */
    private function ownedQueueUrls(string $ownershipPrefix): array
    {
        $owned = [];
        $namespace = "$ownershipPrefix-";

        $pages = $this->sqs->getPaginator('ListQueues', [
            'QueueNamePrefix' => $namespace,
            'MaxResults' => 1000,
        ]);

        foreach ($pages as $result) {
            foreach ($this->stringList($result->get('QueueUrls')) as $queueUrl) {
                $queueName = $this->queueNameFromUrl($queueUrl);

                if (str_starts_with($queueName, $namespace)) {
                    $owned[$queueName] = $queueUrl;
                }
            }
        }

        return $owned;
    }

    private function matchesConfiguredQueueType(string $queueName): bool
    {
        return str_ends_with($queueName, '.fifo') === $this->config->fifo();
    }

    private function queueNameFromUrl(string $queueUrl): string
    {
        $path = parse_url($queueUrl, PHP_URL_PATH);

        return is_string($path) ? rawurldecode(basename($path)) : '';
    }

    private function queueUrl(string $queueName): string
    {
        $queueUrl = $this->sqs->getQueueUrl([
            'QueueName' => $queueName,
            'QueueOwnerAWSAccountId' => $this->config->accountId(),
        ])->get('QueueUrl');

        if (! is_string($queueUrl) || $queueUrl === '') {
            throw SnsSqsTopologyException::incompatibleQueue($queueName, 'GetQueueUrl returned no URL');
        }

        return $queueUrl;
    }

    /**
     * @param  array<string, string>  $attributes
     */
    private function queueArn(string $queue, array $attributes): string
    {
        $queueArn = $attributes['QueueArn'] ?? null;

        if (! is_string($queueArn) || $queueArn === '') {
            throw SnsSqsTopologyException::incompatibleQueue($queue, '[QueueArn] is missing');
        }

        return $queueArn;
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function subscriptionsFor(string $topicArn): array
    {
        $subscriptions = [];

        $pages = $this->sns->getPaginator('ListSubscriptionsByTopic', [
            'TopicArn' => $topicArn,
        ]);

        foreach ($pages as $result) {
            foreach ($this->arrayList($result->get('Subscriptions')) as $subscription) {
                $subscriptions[] = $this->stringKeyedArray($subscription);
            }
        }

        return $subscriptions;
    }

    /**
     * @param  list<array<string, mixed>>  $subscriptions
     */
    private function hasRawSubscription(
        array $subscriptions,
        string $queueName,
        string $queueArn,
    ): bool {
        foreach ($subscriptions as $subscription) {
            if (! $this->isQueueSubscription($subscription, $queueArn)) {
                continue;
            }

            $subscriptionArn = $this->subscriptionArnOrFail($queueName, $subscription);
            $attributes = $this->sns->getSubscriptionAttributes([
                'SubscriptionArn' => $subscriptionArn,
            ])->get('Attributes');
            $attributes = $this->stringMap($attributes);

            if (($attributes['RawMessageDelivery'] ?? null) !== 'true') {
                throw SnsSqsTopologyException::incompatibleSubscription($queueName);
            }

            return true;
        }

        return false;
    }

    /**
     * @param  array<string, mixed>  $subscription
     */
    private function subscriptionArnOrFail(string $queueName, array $subscription): string
    {
        $subscriptionArn = $subscription['SubscriptionArn'] ?? null;

        if (! is_string($subscriptionArn) || ! str_starts_with($subscriptionArn, 'arn:')) {
            throw SnsSqsTopologyException::incompatibleSubscription($queueName);
        }

        return $subscriptionArn;
    }

    /**
     * @param  array<string, mixed>  $subscription
     */
    private function subscriptionArnForQueue(array $subscription, string $queueArn): ?string
    {
        if (! $this->isQueueSubscription($subscription, $queueArn)) {
            return null;
        }

        $subscriptionArn = $subscription['SubscriptionArn'] ?? null;

        if (! is_string($subscriptionArn)) {
            return null;
        }

        return str_starts_with($subscriptionArn, 'arn:') ? $subscriptionArn : null;
    }

    /**
     * @param  array<string, mixed>  $subscription
     */
    private function isQueueSubscription(array $subscription, string $queueArn): bool
    {
        return ($subscription['Protocol'] ?? null) === 'sqs'
            && ($subscription['Endpoint'] ?? null) === $queueArn;
    }

    private function unsubscribeQueue(string $topicArn, string $queueArn): void
    {
        foreach ($this->subscriptionsFor($topicArn) as $subscription) {
            $subscriptionArn = $this->subscriptionArnForQueue($subscription, $queueArn);

            if ($subscriptionArn === null) {
                continue;
            }

            $this->sns->unsubscribe(['SubscriptionArn' => $subscriptionArn]);
        }
    }

    /**
     * @return list<string>
     */
    private function stringList(mixed $value): array
    {
        if (! is_array($value)) {
            return [];
        }

        return array_values(array_filter($value, is_string(...)));
    }

    /**
     * @return list<array<mixed, mixed>>
     */
    private function arrayList(mixed $value): array
    {
        if (! is_array($value)) {
            return [];
        }

        return array_values(array_filter($value, is_array(...)));
    }

    /**
     * @return array<string, string>
     */
    private function stringMap(mixed $value): array
    {
        if (! is_array($value)) {
            return [];
        }

        return array_filter(
            $value,
            static fn (mixed $item, mixed $key): bool => is_string($key) && is_string($item),
            ARRAY_FILTER_USE_BOTH,
        );
    }

    /**
     * @param  array<mixed, mixed>  $value
     * @return array<string, mixed>
     */
    private function stringKeyedArray(array $value): array
    {
        $normalized = [];

        foreach ($value as $key => $item) {
            if (is_string($key)) {
                $normalized[$key] = $item;
            }
        }

        return $normalized;
    }
}
