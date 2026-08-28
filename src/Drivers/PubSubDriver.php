<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Carbon\CarbonImmutable;
use Closure;
use Google\ApiCore\ApiException;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\V1\AcknowledgeRequest;
use Google\Cloud\PubSub\V1\Client\SubscriberClient;
use Google\Cloud\PubSub\V1\ModifyAckDeadlineRequest;
use Google\Cloud\PubSub\V1\PubsubMessage;
use Google\Cloud\PubSub\V1\PullRequest;
use Google\Cloud\PubSub\V1\PullResponse;
use Google\Cloud\PubSub\V1\ReceivedMessage;
use Google\Rpc\Code;
use GuzzleHttp\Handler\CurlMultiHandler;
use InvalidArgumentException;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\CanWaitForConsumerIo;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig;
use Spoolrail\Spoolrail\PubSub\Receipt;
use Spoolrail\Spoolrail\PubSub\ResourceName;
use Spoolrail\Spoolrail\Subscriptions\GuzzleConsumerReactor;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Throwable;
use UnexpectedValueException;

/**
 * @implements Driver<Receipt>
 */
class PubSubDriver implements CanClose, CanManageTopology, CanWaitForConsumerIo, Driver
{
    private const string DEFAULT_ORDERING_KEY = 'spoolrail';

    private const int MAX_ACKNOWLEDGMENT_RETRY_SECONDS = 600;

    /**
     * @var list<array{
     *     due_at: float,
     *     delivery: Delivery<Receipt>,
     *     acknowledged: Closure,
     *     fail: Closure,
     *     attempt: int,
     *     started_at: float
     * }>
     */
    private array $acknowledgmentRetries = [];

    private GuzzleConsumerReactor $consumerReactor;

    public function __construct(
        private ConnectionConfig $config,
        private PubSubClient $publisher,
        private SubscriberClient $subscriber,
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
            $publication = $this->publication($body, $headers, $orderingKey);
            $result = $this->publisher->topic(ResourceName::topic($topic))->publish($publication);
        } catch (InvalidArgumentException $exception) {
            throw PublicationException::notSent($exception);
        } catch (ServiceException $exception) {
            throw $this->publicationFailure($exception);
        } catch (Throwable $exception) {
            throw PublicationException::outcomeUnknown($exception);
        }

        if (! $this->publicationWasAccepted($result)) {
            throw PublicationException::outcomeUnknown(
                new UnexpectedValueException('Google Pub/Sub returned no message ID.'),
            );
        }
    }

    public function receive(
        string $subscription,
        Closure $received,
        Closure $fail,
    ): void {
        $subscriptionPath = $this->subscriptionPath($subscription);

        try {
            $promise = $this->subscriber->pullAsync($this->pullRequest($subscriptionPath));
        } catch (Throwable $exception) {
            $fail(ConsumptionException::consumerStopped($exception));

            return;
        }

        $this->consumerReactor->track(
            $promise,
            function (mixed $response) use ($subscriptionPath, $received, $fail): void {
                try {
                    $deliveries = $this->deliveries(
                        $subscriptionPath,
                        $this->pullResponse($response),
                    );
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

    private function pullRequest(string $subscriptionPath): PullRequest
    {
        return PullRequest::buildFromSubscriptionMaxMessages(
            $subscriptionPath,
            $this->config->receiveBatchSize(),
        );
    }

    private function pullResponse(mixed $response): PullResponse
    {
        if (! $response instanceof PullResponse) {
            throw ConsumptionException::consumerStopped(
                new UnexpectedValueException('Google Pub/Sub returned an invalid pull response.'),
            );
        }

        return $response;
    }

    /**
     * @param  Delivery<Receipt>  $delivery
     */
    public function acknowledge(
        Delivery $delivery,
        Closure $acknowledged,
        Closure $fail,
    ): void {
        $this->attemptAcknowledge(
            $delivery,
            $acknowledged,
            $fail,
            attempt: 0,
            startedAt: microtime(true),
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
            $promise = $this->subscriber->modifyAckDeadlineAsync(
                ModifyAckDeadlineRequest::build(
                    $delivery->receipt->subscriptionPath,
                    [$delivery->receipt->ackId],
                    0,
                ),
            );
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
        $this->startDueAcknowledgmentRetries();
        $this->consumerReactor->wait($this->nextRetryDelayMicroseconds());
        $this->startDueAcknowledgmentRetries();
    }

    public function close(): void
    {
        $this->consumerReactor->cancelPending();
        $this->acknowledgmentRetries = [];
        $this->subscriber->close();
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
    private function publication(
        string $body,
        array $headers,
        ?string $orderingKey,
    ): array {
        $publication = ['data' => $body];

        if ($headers !== []) {
            $publication['attributes'] = $headers;
        }

        if ($orderingKey !== null) {
            $publication['orderingKey'] = $orderingKey;
        } elseif ($this->config->messageOrdering()) {
            $publication['orderingKey'] = self::DEFAULT_ORDERING_KEY;
        }

        return $publication;
    }

    private function publicationFailure(ServiceException $exception): PublicationException
    {
        if (in_array($exception->getCode(), [
            Code::INVALID_ARGUMENT,
            Code::NOT_FOUND,
            Code::ALREADY_EXISTS,
            Code::PERMISSION_DENIED,
            Code::FAILED_PRECONDITION,
            Code::OUT_OF_RANGE,
            Code::UNIMPLEMENTED,
            Code::UNAUTHENTICATED,
        ], true)) {
            return PublicationException::rejected($exception);
        }

        return PublicationException::outcomeUnknown($exception);
    }

    private function publicationWasAccepted(mixed $result): bool
    {
        if (! is_array($result)) {
            return false;
        }

        $messageIds = $result['messageIds'] ?? null;

        return is_array($messageIds)
            && isset($messageIds[0])
            && is_string($messageIds[0])
            && $messageIds[0] !== '';
    }

    private function subscriptionPath(string $subscription): string
    {
        return SubscriberClient::subscriptionName(
            $this->config->projectId(),
            ResourceName::subscription(
                $this->ownershipPrefix->current(),
                $subscription,
            ),
        );
    }

    /**
     * @return list<Delivery<Receipt>>
     */
    private function deliveries(
        string $subscriptionPath,
        PullResponse $response,
    ): array {
        $deliveries = [];

        foreach ($response->getReceivedMessages() as $received) {
            $deliveries[] = $this->delivery($subscriptionPath, $received);
        }

        return $deliveries;
    }

    /** @return Delivery<Receipt> */
    private function delivery(
        string $subscriptionPath,
        ReceivedMessage $received,
    ): Delivery {
        $ackId = $received->getAckId();
        $message = $received->getMessage();

        if ($ackId === '' || $message === null) {
            throw ConsumptionException::consumerStopped(
                new UnexpectedValueException('Google Pub/Sub returned an invalid message delivery.'),
            );
        }

        return new Delivery(
            body: $message->getData(),
            receipt: new Receipt($subscriptionPath, $ackId),
            headers: $this->headers($message),
            transportMessageId: $this->optionalString($message->getMessageId()),
            transportPublishedAt: $this->publishedAt($message),
            redelivered: $this->redelivered($received->getDeliveryAttempt()),
            orderingKey: $this->optionalString($message->getOrderingKey()),
        );
    }

    /** @return array<string, string> */
    private function headers(PubsubMessage $message): array
    {
        $headers = [];

        foreach ($message->getAttributes() as $key => $value) {
            if (is_string($key) && is_string($value)) {
                $headers[$key] = $value;
            }
        }

        return $headers;
    }

    private function optionalString(string $value): ?string
    {
        return $value === '' ? null : $value;
    }

    private function publishedAt(PubsubMessage $message): ?CarbonImmutable
    {
        $publishedAt = $message->getPublishTime();

        return $publishedAt === null
            ? null
            : CarbonImmutable::instance($publishedAt->toDateTime());
    }

    private function redelivered(int $deliveryAttempt): ?bool
    {
        if ($deliveryAttempt === 0) {
            return null;
        }

        return $deliveryAttempt > 1;
    }

    /**
     * @param  Delivery<Receipt>  $delivery
     */
    private function attemptAcknowledge(
        Delivery $delivery,
        Closure $acknowledged,
        Closure $fail,
        int $attempt,
        float $startedAt,
    ): void {
        try {
            $promise = $this->subscriber->acknowledgeAsync(
                AcknowledgeRequest::build(
                    $delivery->receipt->subscriptionPath,
                    [$delivery->receipt->ackId],
                ),
            );
        } catch (Throwable $exception) {
            $this->retryAcknowledgmentOrFail(
                $delivery,
                $acknowledged,
                $fail,
                $attempt,
                $startedAt,
                $exception,
            );

            return;
        }

        $this->consumerReactor->track(
            $promise,
            function () use ($acknowledged): void {
                $acknowledged();
            },
            function (Throwable $exception) use (
                $delivery,
                $acknowledged,
                $fail,
                $attempt,
                $startedAt,
            ): void {
                $this->retryAcknowledgmentOrFail(
                    $delivery,
                    $acknowledged,
                    $fail,
                    $attempt,
                    $startedAt,
                    $exception,
                );
            },
        );
    }

    /**
     * @param  Delivery<Receipt>  $delivery
     */
    private function retryAcknowledgmentOrFail(
        Delivery $delivery,
        Closure $acknowledged,
        Closure $fail,
        int $attempt,
        float $startedAt,
        Throwable $exception,
    ): void {
        if ($this->shouldRetryAcknowledgment($exception, $startedAt)) {
            $this->acknowledgmentRetries[] = [
                'due_at' => microtime(true) + min(64, 2 ** $attempt),
                'delivery' => $delivery,
                'acknowledged' => $acknowledged,
                'fail' => $fail,
                'attempt' => $attempt + 1,
                'started_at' => $startedAt,
            ];

            return;
        }

        $fail(ConsumptionException::settlementFailed($exception));
    }

    private function shouldRetryAcknowledgment(
        Throwable $exception,
        float $startedAt,
    ): bool {
        if (! $this->config->exactlyOnce()
            || microtime(true) - $startedAt >= self::MAX_ACKNOWLEDGMENT_RETRY_SECONDS
            || ! $exception instanceof ApiException) {
            return false;
        }

        return in_array($exception->getCode(), [
            Code::ABORTED,
            Code::CANCELLED,
            Code::DEADLINE_EXCEEDED,
            Code::INTERNAL,
            Code::RESOURCE_EXHAUSTED,
            Code::UNKNOWN,
            Code::UNAVAILABLE,
        ], true);
    }

    private function startDueAcknowledgmentRetries(): void
    {
        $now = microtime(true);
        $pending = [];

        foreach ($this->acknowledgmentRetries as $retry) {
            if ($retry['due_at'] > $now) {
                $pending[] = $retry;

                continue;
            }

            $this->attemptAcknowledge(
                $retry['delivery'],
                $retry['acknowledged'],
                $retry['fail'],
                $retry['attempt'],
                $retry['started_at'],
            );
        }

        $this->acknowledgmentRetries = $pending;
    }

    private function nextRetryDelayMicroseconds(): ?int
    {
        if ($this->acknowledgmentRetries === []) {
            return null;
        }

        $dueAt = min(array_column($this->acknowledgmentRetries, 'due_at'));

        return (int) max(0, ($dueAt - microtime(true)) * 1_000_000);
    }
}
