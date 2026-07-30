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
        if (! LogicalName::isValid($topic)) {
            throw InvalidSubscriptionException::invalidTopic($topic);
        }

        if (! LogicalName::isValid($name)) {
            throw InvalidSubscriptionException::invalidName($name);
        }

        if (! is_a($handler, MessageHandler::class, true)) {
            throw InvalidSubscriptionException::invalidHandler($handler);
        }

        $reflection = new ReflectionClass($handler);

        if ($reflection->isInterface() || $reflection->isAbstract() || $reflection->isEnum()) {
            throw InvalidSubscriptionException::invalidHandler($handler);
        }

        if (isset($this->subscriptions[$name]) || isset($this->drainTargets[$name])) {
            throw InvalidSubscriptionException::duplicateName($name);
        }

        return $this->subscriptions[$name] = new Subscription(
            $topic,
            $name,
            $handler,
            function (string $formerName) use ($name): void {
                $this->registerDrainTarget($formerName, $name);
            },
        );
    }

    public function active(string $name): Subscription
    {
        return $this->subscriptions[$name]
            ?? throw InvalidSubscriptionException::notRegistered($name);
    }

    public function resolveForQueuedMessage(string $name): Subscription
    {
        return $this->active($this->drainTargets[$name] ?? $name);
    }

    /**
     * @return list<Subscription>
     */
    public function all(): array
    {
        return array_values($this->subscriptions);
    }

    /**
     * @return list<Subscription>
     */
    public function forTopicOnConnection(
        string $topic,
        string $connectionName,
        string $defaultConnectionName,
    ): array {
        return array_values(array_filter(
            $this->subscriptions,
            fn (Subscription $subscription): bool => $subscription->topic() === $topic
                && $subscription->connectionName($defaultConnectionName) === $connectionName,
        ));
    }

    private function registerDrainTarget(string $formerName, string $activeName): void
    {
        if (isset($this->subscriptions[$formerName]) || isset($this->drainTargets[$formerName])) {
            throw InvalidSubscriptionException::duplicateName($formerName);
        }

        $this->drainTargets[$formerName] = $activeName;
    }
}
