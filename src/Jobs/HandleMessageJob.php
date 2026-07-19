<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Jobs;

use Illuminate\Contracts\Container\Container;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

class HandleMessageJob
{
    public bool $afterCommit = false;

    public function __construct(
        public readonly Message $message,
        public readonly string $subscription,
    ) {}

    public function handle(SubscriptionRegistry $subscriptions, Container $container): void
    {
        $definition = $subscriptions->get($this->subscription);
        $handler = $container->get($definition->handler());

        $handler->handle($this->message);
    }
}
