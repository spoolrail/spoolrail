<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Closure;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Spoolrail\Spoolrail\Contracts\ClosableDriver;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\Exceptions\SpoolrailException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionFactory;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqName;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Throwable;

class RabbitMqDriver implements ClosableDriver, Driver, ManagedTopology
{
    private ?AbstractConnection $amqpConnection = null;

    private ?AMQPChannel $publisherChannel = null;

    private ?Throwable $handoffFailure = null;

    public function __construct(
        private readonly RabbitMqConnectionConfig $config,
        private readonly RabbitMqConnectionFactory $connectionFactory,
        private readonly ManagedTopology $topology,
        private readonly OwnershipPrefix $ownershipPrefix,
    ) {}

    public function __destruct()
    {
        $this->close();
    }

    public function publish(string $topic, string $body): void
    {
        RabbitMqName::topic($topic);

        try {
            $this->discardIdlePublisherConnection();
            $channel = $this->publisherChannel();
            $message = new AMQPMessage($body, [
                'content_type' => 'application/json',
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            ]);
        } catch (SpoolrailException $exception) {
            $this->discardConnection();

            throw $exception;
        } catch (Throwable $exception) {
            $this->discardConnection();

            throw PublicationException::notSent($exception);
        }

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
     * @param  Closure(string): void  $handoff
     *
     * @throws Throwable
     */
    public function consume(string $subscription, Closure $handoff): void
    {
        $queue = RabbitMqName::queue($this->ownershipPrefix->current(), $subscription);
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
                function (AMQPMessage $delivery) use ($handoff): void {
                    $this->handoff($handoff, $delivery->getBody());
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
        return $this->amqpConnection ??= $this->connectionFactory->create($this->config);
    }

    /**
     * @param  Closure(string): void  $handoff
     */
    private function handoff(Closure $handoff, string $body): void
    {
        try {
            $handoff($body);
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

    private function discardIdlePublisherConnection(): void
    {
        if (! $this->publisherChannel instanceof AMQPChannel || ! $this->amqpConnection instanceof AbstractConnection) {
            return;
        }

        $heartbeat = $this->amqpConnection->getHeartbeat();
        $lastActivity = $this->amqpConnection->getLastActivity();

        if (
            $heartbeat > 0
            && $lastActivity > 0
            && microtime(true) - $lastActivity >= $heartbeat * 2
        ) {
            $this->discardConnection();
        }
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
