<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use ReflectionClass;
use ReflectionException;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Topology\LogicalName;

class SubscriptionRegistry
{
    /** @var array<string, Subscription> */
    private array $subscriptions = [];

    /** @var array<string, string> */
    private array $drainTargets = [];

    /**
     * @param  class-string  $handler
     *
     * @throws InvalidSubscriptionException
     * @throws ReflectionException
     */
    public function subscribe(string $topic, string $name, string $handler): Subscription
    {
        if (! LogicalName::isValidTopic($topic)) {
            throw InvalidSubscriptionException::invalidTopic($topic);
        }

        if (! LogicalName::isValidSubscription($name)) {
            throw InvalidSubscriptionException::invalidName($name);
        }

        $handler = $this->requireConcreteHandler($handler);
        $this->ensureNameIsAvailable($name);

        return $this->subscriptions[$name] = new Subscription(
            $topic,
            $name,
            $handler,
            function (string $formerName) use ($name): void {
                $this->registerDrainTarget($formerName, $name);
            },
        );
    }

    public function findOrFail(string $name): Subscription
    {
        return $this->subscriptions[$name]
            ?? throw InvalidSubscriptionException::notRegistered($name);
    }

    public function resolveForQueuedMessage(string $name): Subscription
    {
        return $this->findOrFail($this->drainTargets[$name] ?? $name);
    }

    /**
     * @return list<Subscription>
     */
    public function all(): array
    {
        return array_values($this->subscriptions);
    }

    /**
     * @param  class-string  $handler
     * @return class-string<MessageHandler>
     *
     * @throws ReflectionException
     */
    private function requireConcreteHandler(string $handler): string
    {
        if (! is_a($handler, MessageHandler::class, true)) {
            throw InvalidSubscriptionException::invalidHandler($handler);
        }

        if (! new ReflectionClass($handler)->isInstantiable()) {
            throw InvalidSubscriptionException::invalidHandler($handler);
        }

        return $handler;
    }

    private function ensureNameIsAvailable(string $name): void
    {
        if (isset($this->subscriptions[$name]) || isset($this->drainTargets[$name])) {
            throw InvalidSubscriptionException::duplicateName($name);
        }
    }

    private function registerDrainTarget(string $formerName, string $activeName): void
    {
        $this->ensureNameIsAvailable($formerName);

        $this->drainTargets[$formerName] = $activeName;
    }
}
