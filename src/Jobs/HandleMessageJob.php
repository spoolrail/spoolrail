<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Jobs;

use Illuminate\Contracts\Container\Container;
use Illuminate\Queue\InteractsWithQueue;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

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
        $definition = $subscriptions->getForQueuedMessage($this->subscription);
        $handler = $container->get($definition->handler());

        $handler->handle($this->message);
    }
}
