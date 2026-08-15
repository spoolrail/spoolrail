<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Closure;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription as PubSubSubscription;
use Google\Rpc\Code;
use InvalidArgumentException;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig;
use Spoolrail\Spoolrail\PubSub\Delivery;
use Spoolrail\Spoolrail\PubSub\ResourceName;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Spoolrail\Spoolrail\TransportContext;
use Throwable;
use UnexpectedValueException;

class PubSubDriver implements CanManageTopology, Driver
{
    private const string DEFAULT_ORDERING_KEY = 'spoolrail';

    public function __construct(
        private ConnectionConfig $config,
        private PubSubClient $publisher,
        private PubSubClient $consumer,
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

    /**
     * @param  Closure(string, TransportContext): void  $handoff
     */
    public function consume(string $subscription, Closure $handoff): void
    {
        $definition = $this->subscriptions->findOrFail($subscription);
        $physicalName = ResourceName::subscription(
            $this->ownershipPrefix->current(),
            $subscription,
        );
        $nativeSubscription = $this->consumer->subscription($physicalName);

        for (; ;) {
            $delivery = $this->receive($nativeSubscription);

            if (! $delivery instanceof Delivery) {
                continue;
            }

            $handoff(
                $delivery->body,
                $this->transportContext($definition, $delivery),
            );

            $this->settle($nativeSubscription, $delivery);
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

    private function receive(PubSubSubscription $subscription): ?Delivery
    {
        try {
            $messages = $subscription->pull(['maxMessages' => 1]);
        } catch (Throwable $exception) {
            throw ConsumptionException::consumerStopped($exception);
        }

        $message = $messages[0] ?? null;

        if ($message === null) {
            return null;
        }

        return Delivery::fromMessage($message);
    }

    private function transportContext(
        Subscription $subscription,
        Delivery $delivery,
    ): TransportContext {
        return new TransportContext(
            driver: 'pubsub',
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

    private function settle(PubSubSubscription $subscription, Delivery $delivery): void
    {
        try {
            $failed = $subscription->acknowledge(
                $delivery->message,
                ['returnFailures' => true],
            );
        } catch (Throwable $exception) {
            throw ConsumptionException::settlementFailed($exception);
        }

        if (is_array($failed) && $failed !== []) {
            throw ConsumptionException::settlementFailed(
                new UnexpectedValueException('Google Pub/Sub acknowledgment failed.'),
            );
        }
    }
}
