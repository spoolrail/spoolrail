<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Closure;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Topology\LogicalName;

class Subscription
{
    private ?string $connectionName = null;

    private ?string $queueConnectionName = null;

    private ?string $queueName = null;

    /**
     * @param  class-string<MessageHandler>  $handlerClass
     * @param  Closure(string): void  $registerDrainTarget
     */
    public function __construct(
        private readonly string $topic,
        private readonly string $name,
        private readonly string $handlerClass,
        private readonly Closure $registerDrainTarget,
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
    public function handlerClass(): string
    {
        return $this->handlerClass;
    }

    public function onConnection(string $connection): self
    {
        $this->connectionName = $this->requireName($connection, 'Spoolrail connection');

        return $this;
    }

    public function onQueueConnection(string $connection): self
    {
        $this->queueConnectionName = $this->requireName($connection, 'Queue connection');

        return $this;
    }

    public function onQueue(string $queue): self
    {
        $this->queueName = $this->requireName($queue, 'Queue');

        return $this;
    }

    public function drainMessagesQueuedFor(string $formerName): self
    {
        if (! LogicalName::isValid($formerName)) {
            throw InvalidSubscriptionException::invalidName($formerName);
        }

        ($this->registerDrainTarget)($formerName);

        return $this;
    }

    public function connectionName(string $defaultConnectionName): string
    {
        return $this->connectionName ?? $defaultConnectionName;
    }

    public function queueConnectionName(): ?string
    {
        return $this->queueConnectionName;
    }

    public function queueName(): ?string
    {
        return $this->queueName;
    }

    private function requireName(string $value, string $label): string
    {
        if (trim($value) === '') {
            throw InvalidSubscriptionException::emptySetting($label);
        }

        return $value;
    }
}
