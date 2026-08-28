<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Carbon\CarbonImmutable;
use Closure;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\CanWaitForConsumerIo;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\Exceptions\SpoolrailException;
use Spoolrail\Spoolrail\RabbitMq\ConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\Connector;
use Spoolrail\Spoolrail\RabbitMq\ResourceName;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Throwable;

/**
 * @implements Driver<AMQPMessage>
 */
class RabbitMqDriver implements CanClose, CanManageTopology, CanWaitForConsumerIo, Driver
{
    private ?AbstractConnection $amqpConnection = null;

    private ?AMQPChannel $publisherChannel = null;

    private ?AMQPChannel $consumerChannel = null;

    /**
     * @var array<string, array{received: Closure, fail: Closure}>
     */
    private array $pendingReceives = [];

    /**
     * @var array<string, list<AMQPMessage>>
     */
    private array $bufferedMessages = [];

    /** @var array<string, true> */
    private array $registeredConsumers = [];

    public function __construct(
        private ConnectionConfig $config,
        private Connector $connector,
        private CanManageTopology $topology,
        private OwnershipPrefix $ownershipPrefix,
        private int $idleWaitMilliseconds = 100,
    ) {}

    public function __destruct()
    {
        $this->close();
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
        ResourceName::topic($topic);

        try {
            $this->discardIdlePublisherConnection();
            $channel = $this->publisherChannel();
            $message = new AMQPMessage(
                $body,
                $this->messageProperties($body, $headers),
            );
        } catch (SpoolrailException $exception) {
            $this->discardConnection();

            throw $exception;
        } catch (Throwable $exception) {
            $this->discardConnection();

            throw PublicationException::notSent($exception);
        }

        $this->publishAndAwaitConfirmation($channel, $message, $topic);
    }

    private function publishAndAwaitConfirmation(
        AMQPChannel $channel,
        AMQPMessage $message,
        string $topic,
    ): void {
        try {
            $channel->basic_publish($message, $topic);
            $channel->wait_for_pending_acks($this->config->publisherConfirmTimeout());
        } catch (PublicationException $exception) {
            $this->discardConnection();

            throw $exception;
        } catch (Throwable $exception) {
            $this->discardConnection();

            throw PublicationException::outcomeUnknown($exception);
        }
    }

    public function receive(
        string $subscription,
        Closure $received,
        Closure $fail,
    ): void {
        $buffered = $this->bufferedMessages[$subscription] ?? [];

        if (($message = array_shift($buffered)) instanceof AMQPMessage) {
            $this->bufferedMessages[$subscription] = $buffered;
            $received([$this->delivery($message)]);

            return;
        }

        $this->pendingReceives[$subscription] = ['received' => $received, 'fail' => $fail];
        $this->ensureConsumerRegistered($subscription);
    }

    private function ensureConsumerRegistered(string $subscription): void
    {
        if (isset($this->registeredConsumers[$subscription])) {
            return;
        }

        try {
            $this->registerConsumer($subscription);
        } catch (Throwable $exception) {
            $this->discardConnection();

            if ($exception instanceof SpoolrailException) {
                throw $exception;
            }

            throw ConsumptionException::consumerStopped($exception);
        }
    }

    /**
     * @param  Delivery<AMQPMessage>  $delivery
     */
    public function acknowledge(
        Delivery $delivery,
        Closure $acknowledged,
        Closure $fail,
    ): void {
        $delivery->receipt->ack();
        $acknowledged();
    }

    /**
     * @param  Delivery<AMQPMessage>  $delivery
     */
    public function release(
        Delivery $delivery,
        Closure $released,
        Closure $fail,
    ): void {
        $delivery->receipt->nack(true);
        $released();
    }

    public function waitForConsumerIo(): void
    {
        try {
            $this->consumerChannel()->wait(
                non_blocking: false,
                timeout: $this->idleWaitMilliseconds / 1_000,
            );
        } catch (AMQPTimeoutException) {
            // The scheduler uses this bounded idle return to revisit every lane.
        } catch (Throwable $exception) {
            $this->discardConnection();

            throw ConsumptionException::consumerStopped($exception);
        }
    }

    public function close(): void
    {
        $this->discardConnection();
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
     * @return list<string> Physical subscription resource names not represented by the declarations
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

    private function publisherChannel(): AMQPChannel
    {
        if ($this->publisherChannel instanceof AMQPChannel) {
            return $this->publisherChannel;
        }

        $channel = $this->amqpConnection()->channel();
        $channel->confirm_select();
        $channel->set_nack_handler(static function (): never {
            throw PublicationException::rejected();
        });

        return $this->publisherChannel = $channel;
    }

    private function amqpConnection(): AbstractConnection
    {
        return $this->amqpConnection ??= $this->connector->connect($this->config);
    }

    private function registerConsumer(string $subscription): void
    {
        $queue = ResourceName::queue($this->ownershipPrefix->current(), $subscription);
        $channel = $this->consumerChannel();
        $channel->basic_consume(
            $queue,
            '',
            false,
            false,
            false,
            false,
            function (AMQPMessage $message) use ($subscription): void {
                $this->receiveMessage($subscription, $message);
            },
        );

        $this->registeredConsumers[$subscription] = true;
    }

    private function consumerChannel(): AMQPChannel
    {
        if ($this->consumerChannel instanceof AMQPChannel) {
            return $this->consumerChannel;
        }

        $channel = $this->amqpConnection()->channel();
        $channel->basic_qos(0, $this->config->prefetch(), false);

        return $this->consumerChannel = $channel;
    }

    private function receiveMessage(string $subscription, AMQPMessage $message): void
    {
        $operation = $this->pendingReceives[$subscription] ?? null;

        if ($operation === null) {
            $this->bufferedMessages[$subscription][] = $message;

            return;
        }

        unset($this->pendingReceives[$subscription]);
        ($operation['received'])([$this->delivery($message)]);
    }

    /**
     * @return Delivery<AMQPMessage>
     */
    private function delivery(AMQPMessage $message): Delivery
    {
        $messageId = $this->property($message, 'message_id');
        $timestamp = $this->property($message, 'timestamp');

        return new Delivery(
            body: $message->getBody(),
            receipt: $message,
            headers: $this->headers($message),
            transportMessageId: is_string($messageId) ? $messageId : null,
            transportPublishedAt: is_int($timestamp)
                ? CarbonImmutable::createFromTimestampUTC($timestamp)
                : null,
            redelivered: $message->isRedelivered(),
        );
    }

    private function property(AMQPMessage $message, string $name): mixed
    {
        return $message->has($name) ? $message->get($name) : null;
    }

    /**
     * @param  array<string, string>  $headers
     * @return array<string, mixed>
     */
    private function messageProperties(string $body, array $headers): array
    {
        /** @var array{id: string, type: string, published_at: string} $envelope */
        $envelope = json_decode($body, true, flags: JSON_THROW_ON_ERROR);

        $properties = [
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'message_id' => $envelope['id'],
            'type' => $envelope['type'],
            'timestamp' => CarbonImmutable::parse($envelope['published_at'])->getTimestamp(),
        ];

        if ($headers !== []) {
            $properties['application_headers'] = new AMQPTable($headers);
        }

        return $properties;
    }

    /**
     * @return array<string, mixed>
     */
    private function headers(AMQPMessage $message): array
    {
        if (! $message->has('application_headers')) {
            return [];
        }

        $headers = $message->get('application_headers');

        if (! $headers instanceof AMQPTable) {
            return [];
        }

        $nativeHeaders = [];

        foreach ($headers->getNativeData() as $key => $value) {
            if (is_string($key)) {
                $nativeHeaders[$key] = $value;
            }
        }

        return $nativeHeaders;
    }

    private function discardIdlePublisherConnection(): void
    {
        $connection = $this->amqpConnection;

        if (! $this->publisherChannel instanceof AMQPChannel || ! $connection instanceof AbstractConnection) {
            return;
        }

        if ($this->isPublisherConnectionIdle($connection)) {
            $this->discardConnection();
        }
    }

    private function isPublisherConnectionIdle(AbstractConnection $connection): bool
    {
        $heartbeat = $connection->getHeartbeat();
        $lastActivity = $connection->getLastActivity();

        return $heartbeat > 0
            && $lastActivity > 0
            && microtime(true) - $lastActivity >= $heartbeat * 2;
    }

    private function discardConnection(): void
    {
        $amqpConnection = $this->amqpConnection;
        $this->amqpConnection = null;
        $this->publisherChannel = null;
        $this->consumerChannel = null;
        $this->pendingReceives = [];
        $this->bufferedMessages = [];
        $this->registeredConsumers = [];

        if (! $amqpConnection instanceof AbstractConnection) {
            return;
        }

        try {
            $amqpConnection->close();
        } catch (Throwable) {
            // Preserve the operation failure that made this connection unusable.
        }
    }
}
