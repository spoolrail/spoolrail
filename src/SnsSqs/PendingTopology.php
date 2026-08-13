<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\SnsSqs;

use Aws\Exception\AwsException;
use Aws\Result;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use Closure;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\SnsSqsTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Throwable;

class PendingTopology implements TopologyPlan
{
    /**
     * @var list<array{name: string, arn: string, attributes: array<string, string>}>
     */
    private array $topics = [];

    /**
     * @var list<array{name: string, attributes: array<string, string>}>
     */
    private array $queues = [];

    /**
     * @var list<array{url: string, policy: string}>
     */
    private array $queuePolicies = [];

    /**
     * @var list<array{topic_arn: string, queue_arn: string}>
     */
    private array $subscriptions = [];

    /**
     * @var array<string, list<array<string, mixed>>>
     */
    private array $topicSubscriptions = [];

    public function __construct(
        private SnsClient $sns,
        private SqsClient $sqs,
    ) {}

    public function apply(): void
    {
        $this->createTopics();
        $this->createQueues();
        $this->updateQueuePolicies();
        $this->createSubscriptions();
    }

    /**
     * @param  array<string, string>  $attributes
     */
    public function addTopic(string $name, string $arn, array $attributes): void
    {
        $this->topics[] = [
            'name' => $name,
            'arn' => $arn,
            'attributes' => $attributes,
        ];
    }

    /**
     * @param  list<array<string, mixed>>  $subscriptions
     */
    public function rememberTopicSubscriptions(string $topicArn, array $subscriptions): void
    {
        $this->topicSubscriptions[$topicArn] = $subscriptions;
    }

    public function hasInspectedTopic(string $topicArn): bool
    {
        return array_key_exists($topicArn, $this->topicSubscriptions);
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function subscriptionsFor(string $topicArn): array
    {
        return $this->topicSubscriptions[$topicArn];
    }

    /**
     * @param  array<string, string>  $attributes
     */
    public function addQueue(
        string $name,
        array $attributes,
    ): void {
        $this->queues[] = [
            'name' => $name,
            'attributes' => $attributes,
        ];
    }

    public function addQueuePolicy(string $url, string $policy): void
    {
        $this->queuePolicies[] = [
            'url' => $url,
            'policy' => $policy,
        ];
    }

    public function addSubscription(string $topicArn, string $queueArn): void
    {
        $this->subscriptions[] = [
            'topic_arn' => $topicArn,
            'queue_arn' => $queueArn,
        ];
    }

    public function createsResources(): bool
    {
        return $this->topics !== [] || $this->queues !== [];
    }

    private function createTopics(): void
    {
        foreach ($this->topics as $topic) {
            $createdArn = $this->applyRequest(
                "creating SNS topic [{$topic['name']}]",
                fn (): Result => $this->sns->createTopic([
                    'Name' => $topic['name'],
                    'Attributes' => $topic['attributes'],
                ]),
            )->get('TopicArn');

            if ($createdArn !== $topic['arn']) {
                throw SnsSqsTopologyException::unexpectedCreatedResource(
                    $topic['arn'],
                    is_string($createdArn) ? $createdArn : 'unknown',
                );
            }
        }
    }

    private function createQueues(): void
    {
        foreach ($this->queues as $queue) {
            $this->applyRequest(
                "creating SQS queue [{$queue['name']}]",
                fn (): Result => $this->sqs->createQueue([
                    'QueueName' => $queue['name'],
                    'Attributes' => $queue['attributes'],
                ]),
            );
        }
    }

    private function updateQueuePolicies(): void
    {
        foreach ($this->queuePolicies as $policy) {
            $this->applyRequest(
                "updating SQS queue [{$policy['url']}]",
                fn (): Result => $this->sqs->setQueueAttributes([
                    'QueueUrl' => $policy['url'],
                    'Attributes' => ['Policy' => $policy['policy']],
                ]),
            );
        }
    }

    private function createSubscriptions(): void
    {
        foreach ($this->subscriptions as $subscription) {
            $this->applyRequest(
                "subscribing SQS queue [{$subscription['queue_arn']}] to SNS topic [{$subscription['topic_arn']}]",
                fn (): Result => $this->sns->subscribe([
                    'TopicArn' => $subscription['topic_arn'],
                    'Protocol' => 'sqs',
                    'Endpoint' => $subscription['queue_arn'],
                    'Attributes' => ['RawMessageDelivery' => 'true'],
                    'ReturnSubscriptionArn' => true,
                ]),
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
        } catch (AwsException $exception) {
            $failure = SnsSqsTopologyException::operationFailed($operation, $exception);

            if ($failure->shouldRetry()) {
                throw TopologySyncRequiresRetryException::afterFailure($failure);
            }

            throw $failure;
        } catch (Throwable $exception) {
            throw SnsSqsTopologyException::operationFailed($operation, $exception);
        }
    }
}
