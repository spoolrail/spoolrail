<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Jobs;

use Illuminate\Container\Container;
use Illuminate\Queue\InteractsWithQueue;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Throwable;

class HandleMessageJob
{
    use InteractsWithQueue;

    public bool $afterCommit = false;

    public mixed $tries = null;

    public mixed $backoff = null;

    public mixed $maxExceptions = null;

    public mixed $timeout = null;

    public mixed $failOnTimeout = false;

    public mixed $retryUntil = null;

    public mixed $middleware = [];

    public function __construct(
        public readonly Message $message,
        public readonly string $subscription,
    ) {}

    public function handle(SubscriptionRegistry $subscriptions, Container $container): void
    {
        $subscription = $subscriptions->resolveForQueuedMessage($this->subscription);
        $handler = $container->get($subscription->handlerClass());

        $handler->handle($this->message);
    }

    public function failed(?Throwable $exception): void
    {
        $container = Container::getInstance();
        $subscriptions = $container->get(SubscriptionRegistry::class);

        try {
            $handlerClass = $subscriptions
                ->resolveForQueuedMessage($this->subscription)
                ->handlerClass();
        } catch (InvalidSubscriptionException) {
            return;
        }

        if (! method_exists($handlerClass, 'failed')) {
            return;
        }

        $handler = $container->get($handlerClass);

        if (! method_exists($handler, 'failed')) {
            return;
        }

        $handler->failed($this->message, $exception);
    }
}
