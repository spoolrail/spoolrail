<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Aws\Exception\AwsException;
use Aws\Exception\CredentialsException;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use Closure;
use InvalidArgumentException;
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
            $delivery = $this->receive($queueUrl);

            if (! $delivery instanceof Delivery) {
                continue;
            }

            $handoff(
                $delivery->body,
                $this->transportContext($definition, $delivery),
            );

            $this->settle($queueUrl, $delivery);
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
        if (intdiv($exception->getStatusCode() ?? 0, 100) === 4) {
            return PublicationException::rejected($exception);
        }

        return PublicationException::outcomeUnknown($exception);
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

    private function receive(string $queueUrl): ?Delivery
    {
        try {
            $messages = $this->sqs->receiveMessage([
                'QueueUrl' => $queueUrl,
                'MaxNumberOfMessages' => 1,
                'WaitTimeSeconds' => 20,
                'AttributeNames' => ['All'],
                'MessageAttributeNames' => ['All'],
            ])->get('Messages');
        } catch (Throwable $exception) {
            throw ConsumptionException::consumerStopped($exception);
        }

        return Delivery::first($messages);
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
