<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Carbon\CarbonImmutable;
use Closure;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\Exceptions\SpoolrailException;
use Spoolrail\Spoolrail\RabbitMq\ConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\Connector;
use Spoolrail\Spoolrail\RabbitMq\ResourceName;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Spoolrail\Spoolrail\TransportContext;
use Throwable;

class RabbitMqDriver implements CanClose, CanManageTopology, Driver
{
    private ?AbstractConnection $amqpConnection = null;

    private ?AMQPChannel $publisherChannel = null;

    private ?Throwable $handoffFailure = null;

    public function __construct(
        private ConnectionConfig $config,
        private Connector $connector,
        private CanManageTopology $topology,
        private OwnershipPrefix $ownershipPrefix,
    ) {}

    public function __destruct()
    {
        $this->close();
    }

    /**
     * @param  array<string, string>  $headers
     */
    public function publish(string $topic, string $body, array $headers): void
    {
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

    /**
     * @param  Closure(string, TransportContext): void  $handoff
     *
     * @throws Throwable
     */
    public function consume(string $subscription, Closure $handoff): void
    {
        $queue = ResourceName::queue($this->ownershipPrefix->current(), $subscription);
        $this->handoffFailure = null;

        try {
            $channel = $this->amqpConnection()->channel();
            $channel->basic_qos(0, $this->config->prefetch(), false);
            $channel->basic_consume(
                $queue,
                '',
                false,
                false,
                false,
                false,
                function (AMQPMessage $delivery) use ($handoff, $subscription): void {
                    $this->handoff($handoff, $delivery, $subscription);
                    $this->acknowledge($delivery);
                },
            );

            $channel->consume();

            throw ConsumptionException::consumerStopped();
        } catch (Throwable $exception) {
            $this->discardConnection();

            if ($exception === $this->handoffFailure || $exception instanceof SpoolrailException) {
                throw $exception;
            }

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

    /**
     * @param  Closure(string, TransportContext): void  $handoff
     */
    private function handoff(
        Closure $handoff,
        AMQPMessage $delivery,
        string $subscription,
    ): void {
        try {
            $handoff(
                $delivery->getBody(),
                new TransportContext(
                    driver: 'rabbitmq',
                    connectionName: $this->config->connectionName,
                    topic: (string) $delivery->getExchange(),
                    subscription: $subscription,
                    headers: $this->headers($delivery),
                    redelivered: $delivery->isRedelivered(),
                ),
            );
        } catch (Throwable $exception) {
            $this->handoffFailure = $exception;

            throw $exception;
        }
    }

    private function acknowledge(AMQPMessage $delivery): void
    {
        try {
            $delivery->ack();
        } catch (Throwable $exception) {
            throw ConsumptionException::settlementFailed($exception);
        }
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
    private function headers(AMQPMessage $delivery): array
    {
        if (! $delivery->has('application_headers')) {
            return [];
        }

        $headers = $delivery->get('application_headers');

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
