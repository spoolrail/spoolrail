<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Aws\Exception\AwsException;
use Aws\Exception\CredentialsException;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use Closure;
use InvalidArgumentException;
use Ramsey\Uuid\Uuid;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\SnsSqs\ConnectionConfig;
use Spoolrail\Spoolrail\SnsSqs\Delivery;
use Spoolrail\Spoolrail\SnsSqs\ResourceName;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Spoolrail\Spoolrail\TransportContext;
use Throwable;
use UnexpectedValueException;

class SnsSqsDriver implements CanManageTopology, Driver
{
    private const string DEFAULT_MESSAGE_GROUP = 'spoolrail';

    public function __construct(
        private ConnectionConfig $config,
        private SnsClient $sns,
        private SqsClient $sqs,
        private CanManageTopology $topology,
        private OwnershipPrefix $ownershipPrefix,
        private SubscriptionRegistry $subscriptions,
    ) {}

    /**
     * @param  array<string, string>  $headers
     */
    public function publish(
        string $topic,
        string $body,
        array $headers,
        ?string $orderingKey = null,
    ): void {
        try {
            $request = $this->publicationRequest($topic, $body, $headers, $orderingKey);
        } catch (Throwable $exception) {
            throw PublicationException::notSent($exception);
        }

        try {
            $this->sns->publish($request);
        } catch (CredentialsException|InvalidArgumentException $exception) {
            throw PublicationException::notSent($exception);
        } catch (AwsException $exception) {
            throw $this->publicationFailure($exception);
        } catch (Throwable $exception) {
            throw PublicationException::outcomeUnknown($exception);
        }
    }

    /**
     * @param  Closure(string, TransportContext): void  $handoff
     */
    public function consume(string $subscription, Closure $handoff): void
    {
        $definition = $this->subscriptions->findOrFail($subscription);
        $queueName = ResourceName::queue(
            $this->ownershipPrefix->current(),
            $subscription,
            $this->config->fifo(),
        );

        $queueUrl = $this->queueUrl($queueName);

        for (; ;) {
            foreach ($this->receive($queueUrl) as $delivery) {
                $handoff(
                    $delivery->body,
                    $this->transportContext($definition, $delivery),
                );

                $this->settle($queueUrl, $delivery);
            }
        }
    }

    /**
     * @param  list<Subscription>  $subscriptions
     */
    public function planSync(array $subscriptions, string $ownershipPrefix): TopologyPlan
    {
        return $this->topology->planSync($subscriptions, $ownershipPrefix);
    }

    /**
     * @param  list<Subscription>  $subscriptions
     * @return list<string>
     */
    public function undeclaredSubscriptionResourceNames(
        array $subscriptions,
        string $ownershipPrefix,
    ): array {
        return $this->topology->undeclaredSubscriptionResourceNames(
            $subscriptions,
            $ownershipPrefix,
        );
    }

    public function deleteSubscription(string $physicalName): void
    {
        $this->topology->deleteSubscription($physicalName);
    }

    public function deleteTopic(string $topic): void
    {
        $this->topology->deleteTopic($topic);
    }

    /**
     * @param  array<string, string>  $headers
     * @return array<string, mixed>
     */
    private function publicationRequest(
        string $topic,
        string $body,
        array $headers,
        ?string $orderingKey,
    ): array {
        /** @var array{id: string} $envelope */
        $envelope = json_decode($body, true, flags: JSON_THROW_ON_ERROR);

        $request = [
            'TopicArn' => ResourceName::topicArn($this->config, $topic),
            'Message' => $body,
        ];

        if ($headers !== []) {
            $request['MessageAttributes'] = array_map(
                static fn (string $value): array => [
                    'DataType' => 'String',
                    'StringValue' => $value,
                ],
                $headers,
            );
        }

        if ($this->config->fifo()) {
            $request['MessageGroupId'] = $orderingKey ?? self::DEFAULT_MESSAGE_GROUP;
            $request['MessageDeduplicationId'] = $envelope['id'];
        } elseif ($orderingKey !== null) {
            $request['MessageGroupId'] = $orderingKey;
        }

        return $request;
    }

    private function publicationFailure(AwsException $exception): PublicationException
    {
        if ($this->publicationWasThrottled($exception)) {
            return PublicationException::notSent($exception);
        }

        if ($this->publicationWasRejected($exception)) {
            return PublicationException::rejected($exception);
        }

        return PublicationException::outcomeUnknown($exception);
    }

    private function publicationWasThrottled(AwsException $exception): bool
    {
        $errorCode = (string) $exception->getAwsErrorCode();
        if ($exception->getStatusCode() === 429) {
            return true;
        }
        if (stripos($errorCode, 'throttl') !== false) {
            return true;
        }

        return in_array($errorCode, ['RequestLimitExceeded', 'TooManyRequestsException'], true);
    }

    private function publicationWasRejected(AwsException $exception): bool
    {
        $status = $exception->getStatusCode();

        return $status !== null && $status >= 400 && $status < 500 && $status !== 408;
    }

    private function queueUrl(string $queueName): string
    {
        try {
            $queueUrl = $this->sqs->getQueueUrl([
                'QueueName' => $queueName,
                'QueueOwnerAWSAccountId' => $this->config->accountId(),
            ])->get('QueueUrl');
        } catch (Throwable $exception) {
            throw ConsumptionException::consumerStopped($exception);
        }

        if (! is_string($queueUrl) || $queueUrl === '') {
            throw ConsumptionException::consumerStopped(
                new UnexpectedValueException('SQS GetQueueUrl returned no queue URL.'),
            );
        }

        return $queueUrl;
    }

    /**
     * @return list<Delivery>
     */
    private function receive(string $queueUrl): array
    {
        $request = [
            'QueueUrl' => $queueUrl,
            'MaxNumberOfMessages' => $this->config->receiveBatchSize(),
            'WaitTimeSeconds' => 20,
            'AttributeNames' => ['All'],
            'MessageAttributeNames' => ['All'],
        ];

        if ($this->config->fifo()) {
            $request['ReceiveRequestAttemptId'] = Uuid::uuid4()->toString();
        }

        try {
            $messages = $this->sqs->receiveMessage($request)->get('Messages');
        } catch (Throwable $exception) {
            throw ConsumptionException::consumerStopped($exception);
        }

        return Delivery::fromMessages($messages);
    }

    private function transportContext(
        Subscription $subscription,
        Delivery $delivery,
    ): TransportContext {
        return new TransportContext(
            driver: 'snssqs',
            connectionName: $this->config->connectionName,
            topic: $subscription->topic(),
            subscription: $subscription->name(),
            headers: $delivery->headers(),
            transportMessageId: $delivery->transportMessageId(),
            transportPublishedAt: $delivery->publishedAt(),
            redelivered: $delivery->wasRedelivered(),
            orderingKey: $delivery->orderingKey(),
        );
    }

    private function settle(string $queueUrl, Delivery $delivery): void
    {
        try {
            $this->sqs->deleteMessage([
                'QueueUrl' => $queueUrl,
                'ReceiptHandle' => $delivery->receiptHandle,
            ]);
        } catch (Throwable $exception) {
            throw ConsumptionException::settlementFailed($exception);
        }
    }
}
