<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Aws\Exception\AwsException;
use Aws\Exception\CredentialsException;
use Aws\ResultInterface;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use Carbon\CarbonImmutable;
use Closure;
use GuzzleHttp\Handler\CurlMultiHandler;
use InvalidArgumentException;
use Ramsey\Uuid\Uuid;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\CanWaitForConsumerIo;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\SnsSqs\ConnectionConfig;
use Spoolrail\Spoolrail\SnsSqs\Receipt;
use Spoolrail\Spoolrail\SnsSqs\ResourceName;
use Spoolrail\Spoolrail\Subscriptions\GuzzleConsumerReactor;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Throwable;
use UnexpectedValueException;

/**
 * @implements Driver<Receipt>
 */
class SnsSqsDriver implements CanClose, CanManageTopology, CanWaitForConsumerIo, Driver
{
    private const string DEFAULT_MESSAGE_GROUP = 'spoolrail';

    /** @var array<string, string> */
    private array $queueUrls = [];

    private GuzzleConsumerReactor $consumerReactor;

    public function __construct(
        private ConnectionConfig $config,
        private SnsClient $sns,
        private SqsClient $sqs,
        private CanManageTopology $topology,
        private OwnershipPrefix $ownershipPrefix,
        CurlMultiHandler $httpHandler,
        int $idleWaitMilliseconds = 100,
    ) {
        $this->consumerReactor = new GuzzleConsumerReactor(
            $httpHandler,
            $idleWaitMilliseconds,
        );
    }

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

    public function receive(
        string $subscription,
        Closure $received,
        Closure $fail,
    ): void {
        try {
            $queueUrl = $this->queueUrl($subscription);
            $promise = $this->sqs->receiveMessageAsync($this->receiveRequest($queueUrl));
        } catch (Throwable $exception) {
            $fail(ConsumptionException::consumerStopped($exception));

            return;
        }

        $this->consumerReactor->track(
            $promise,
            function (ResultInterface $result) use ($queueUrl, $received, $fail): void {
                try {
                    $deliveries = $this->deliveries($queueUrl, $result->get('Messages'));
                } catch (Throwable $exception) {
                    $fail($exception);

                    return;
                }

                $received($deliveries);
            },
            function (Throwable $exception) use ($fail): void {
                $fail(ConsumptionException::consumerStopped($exception));
            },
        );
    }

    /** @return array<string, mixed> */
    private function receiveRequest(string $queueUrl): array
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

        return $request;
    }

    /**
     * @param  Delivery<Receipt>  $delivery
     */
    public function acknowledge(
        Delivery $delivery,
        Closure $acknowledged,
        Closure $fail,
    ): void {
        try {
            $promise = $this->sqs->deleteMessageAsync([
                'QueueUrl' => $delivery->receipt->queueUrl,
                'ReceiptHandle' => $delivery->receipt->handle,
            ]);
        } catch (Throwable $exception) {
            $fail(ConsumptionException::settlementFailed($exception));

            return;
        }

        $this->consumerReactor->track(
            $promise,
            function () use ($acknowledged): void {
                $acknowledged();
            },
            function (Throwable $exception) use ($fail): void {
                $fail(ConsumptionException::settlementFailed($exception));
            },
        );
    }

    /**
     * @param  Delivery<Receipt>  $delivery
     */
    public function release(
        Delivery $delivery,
        Closure $released,
        Closure $fail,
    ): void {
        try {
            $promise = $this->sqs->changeMessageVisibilityAsync([
                'QueueUrl' => $delivery->receipt->queueUrl,
                'ReceiptHandle' => $delivery->receipt->handle,
                'VisibilityTimeout' => 0,
            ]);
        } catch (Throwable $exception) {
            $fail(ConsumptionException::settlementFailed($exception));

            return;
        }

        $this->consumerReactor->track(
            $promise,
            function () use ($released): void {
                $released();
            },
            function (Throwable $exception) use ($fail): void {
                $fail(ConsumptionException::settlementFailed($exception));
            },
        );
    }

    public function waitForConsumerIo(): void
    {
        $this->consumerReactor->wait();
    }

    public function close(): void
    {
        $this->consumerReactor->cancelPending();
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

    private function queueUrl(string $subscription): string
    {
        if (isset($this->queueUrls[$subscription])) {
            return $this->queueUrls[$subscription];
        }

        $queueName = ResourceName::queue(
            $this->ownershipPrefix->current(),
            $subscription,
            $this->config->fifo(),
        );

        $queueUrl = $this->findQueueUrl($queueName);

        if (! is_string($queueUrl) || $queueUrl === '') {
            throw ConsumptionException::consumerStopped(
                new UnexpectedValueException('SQS GetQueueUrl returned no queue URL.'),
            );
        }

        return $this->queueUrls[$subscription] = $queueUrl;
    }

    private function findQueueUrl(string $queueName): mixed
    {
        try {
            return $this->sqs->getQueueUrl([
                'QueueName' => $queueName,
                'QueueOwnerAWSAccountId' => $this->config->accountId(),
            ])->get('QueueUrl');
        } catch (Throwable $exception) {
            throw ConsumptionException::consumerStopped($exception);
        }
    }

    /**
     * @return list<Delivery<Receipt>>
     */
    private function deliveries(string $queueUrl, mixed $messages): array
    {
        if (! is_array($messages)) {
            return [];
        }

        $deliveries = [];

        foreach ($messages as $message) {
            $deliveries[] = $this->delivery($queueUrl, $message);
        }

        return $deliveries;
    }

    /** @return Delivery<Receipt> */
    private function delivery(string $queueUrl, mixed $message): Delivery
    {
        if (! is_array($message)) {
            throw ConsumptionException::consumerStopped(
                new UnexpectedValueException('SQS returned an invalid message delivery.'),
            );
        }

        $attributes = $this->messageMap($message, 'Attributes');
        $sentTimestamp = $this->optionalString($attributes, 'SentTimestamp');
        $receiveCount = $this->optionalString($attributes, 'ApproximateReceiveCount');

        return new Delivery(
            body: $this->requiredMessageString($message, 'Body'),
            receipt: new Receipt(
                $queueUrl,
                $this->requiredMessageString($message, 'ReceiptHandle'),
            ),
            headers: $this->messageHeaders($this->messageMap($message, 'MessageAttributes')),
            transportMessageId: $this->optionalString($message, 'MessageId'),
            transportPublishedAt: $sentTimestamp !== null && ctype_digit($sentTimestamp)
                ? CarbonImmutable::createFromTimestampMsUTC((int) $sentTimestamp)
                : null,
            redelivered: $receiveCount !== null && ctype_digit($receiveCount)
                ? (int) $receiveCount > 1
                : null,
            orderingKey: $this->optionalString($attributes, 'MessageGroupId'),
        );
    }

    /** @param  array<array-key, mixed>  $values */
    private function optionalString(array $values, string $key): ?string
    {
        $value = $values[$key] ?? null;

        return is_string($value) ? $value : null;
    }

    /**
     * @param  array<array-key, mixed>  $message
     */
    private function requiredMessageString(array $message, string $key): string
    {
        $value = $message[$key] ?? null;

        if (! is_string($value) || $value === '') {
            throw ConsumptionException::consumerStopped(
                new UnexpectedValueException('SQS returned a delivery without a body or receipt handle.'),
            );
        }

        return $value;
    }

    /**
     * @param  array<array-key, mixed>  $message
     * @return array<array-key, mixed>
     */
    private function messageMap(array $message, string $key): array
    {
        $value = $message[$key] ?? null;

        return is_array($value) ? $value : [];
    }

    /**
     * @param  array<array-key, mixed>  $attributes
     * @return array<string, mixed>
     */
    private function messageHeaders(array $attributes): array
    {
        $headers = [];

        foreach ($attributes as $name => $attribute) {
            if (! is_string($name)) {
                continue;
            }
            if (! is_array($attribute)) {
                continue;
            }
            $headers[$name] = $attribute['StringValue']
                ?? $attribute['BinaryValue']
                ?? $attribute;
        }

        return $headers;
    }
}
