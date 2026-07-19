<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;

class SubscriptionRegistry
{
    /** @var array<string, Subscription> */
    private array $subscriptions = [];

    /**
     * @param  class-string  $handler
     */
    public function subscribe(string $topic, string $name, string $handler): Subscription
    {
        if (trim($topic) === '') {
            throw InvalidSubscriptionException::emptyTopic();
        }

        if (trim($name) === '') {
            throw InvalidSubscriptionException::emptyName();
        }

        if (! is_a($handler, MessageHandler::class, true)) {
            throw InvalidSubscriptionException::invalidHandler($handler);
        }

        if (isset($this->subscriptions[$name])) {
            throw InvalidSubscriptionException::duplicateName($name);
        }

        return $this->subscriptions[$name] = new Subscription($topic, $name, $handler);
    }

    public function get(string $name): Subscription
    {
        return $this->subscriptions[$name]
            ?? throw InvalidSubscriptionException::notRegistered($name);
    }

    /**
     * @return list<Subscription>
     */
    public function forTopic(string $topic, string $connection, string $defaultConnection): array
    {
        return array_values(array_filter(
            $this->subscriptions,
            fn (Subscription $subscription): bool => $subscription->topic() === $topic
                && $subscription->connection($defaultConnection) === $connection,
        ));
    }
}
