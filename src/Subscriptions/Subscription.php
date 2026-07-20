<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Closure;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;

class Subscription
{
    private ?string $connection = null;

    private ?string $queueConnection = null;

    private ?string $queue = null;

    /**
     * @param  class-string<MessageHandler>  $handler
     * @param  Closure(string): void  $registerQueuedMessageSubscription
     */
    public function __construct(
        private readonly string $topic,
        private readonly string $name,
        private readonly string $handler,
        private readonly Closure $registerQueuedMessageSubscription,
    ) {}

    public function topic(): string
    {
        return $this->topic;
    }

    public function name(): string
    {
        return $this->name;
    }

    /**
     * @return class-string<MessageHandler>
     */
    public function handler(): string
    {
        return $this->handler;
    }

    public function onConnection(string $connection): self
    {
        $this->connection = $this->requireName($connection, 'Spoolrail connection');

        return $this;
    }

    public function onQueueConnection(string $connection): self
    {
        $this->queueConnection = $this->requireName($connection, 'Queue connection');

        return $this;
    }

    public function onQueue(string $queue): self
    {
        $this->queue = $this->requireName($queue, 'Queue');

        return $this;
    }

    public function drainMessagesQueuedFor(string $subscription): self
    {
        ($this->registerQueuedMessageSubscription)(
            $this->requireName($subscription, 'Queued message subscription name'),
        );

        return $this;
    }

    public function connection(string $default): string
    {
        return $this->connection ?? $default;
    }

    public function queueConnection(): ?string
    {
        return $this->queueConnection;
    }

    public function queue(): ?string
    {
        return $this->queue;
    }

    private function requireName(string $value, string $label): string
    {
        if (trim($value) === '') {
            throw InvalidSubscriptionException::emptySetting($label);
        }

        return $value;
    }
}
