<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Closure;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

/**
 * @phpstan-type MessageRecord array{
 *     body: string,
 *     headers: array<string, string>,
 *     redelivered: bool
 * }
 * @phpstan-type Receipt array{subscription: string, message: MessageRecord}
 *
 * @implements Driver<Receipt>
 */
class ArrayDriver implements Driver
{
    /** @var array<string, list<MessageRecord>> */
    private array $messages = [];

    public function __construct(
        private string $connectionName,
        private string $defaultConnectionName,
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
        foreach ($this->matchingSubscriptions($topic) as $subscription) {
            $this->messages[$subscription->name()][] = [
                'body' => $body,
                'headers' => $headers,
                'redelivered' => false,
            ];
        }
    }

    public function receive(
        string $subscription,
        Closure $received,
        Closure $fail,
    ): void {
        $message = $this->reserveNextMessage($subscription);

        if ($message === null) {
            $received([]);

            return;
        }

        $received([new Delivery(
            body: $message['body'],
            receipt: [
                'subscription' => $subscription,
                'message' => $message,
            ],
            headers: $message['headers'],
            redelivered: $message['redelivered'],
        )]);
    }

    /**
     * @param  Delivery<Receipt>  $delivery
     */
    public function acknowledge(
        Delivery $delivery,
        Closure $acknowledged,
        Closure $fail,
    ): void {
        $acknowledged();
    }

    /**
     * @param  Delivery<Receipt>  $delivery
     */
    public function release(
        Delivery $delivery,
        Closure $released,
        Closure $fail,
    ): void {
        $message = $delivery->receipt['message'];
        $message['redelivered'] = true;

        array_unshift(
            $this->messages[$delivery->receipt['subscription']],
            $message,
        );

        $released();
    }

    /** @return MessageRecord|null */
    private function reserveNextMessage(string $subscription): ?array
    {
        if (($this->messages[$subscription] ?? []) === []) {
            return null;
        }

        return array_shift($this->messages[$subscription]);
    }

    /**
     * @return list<Subscription>
     */
    private function matchingSubscriptions(string $topic): array
    {
        return array_values(array_filter(
            $this->subscriptions->all(),
            fn (Subscription $subscription): bool => $subscription->topic() === $topic
                && $subscription->connectionName($this->defaultConnectionName) === $this->connectionName,
        ));
    }
}
