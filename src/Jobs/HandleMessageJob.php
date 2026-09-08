<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Jobs;

use Illuminate\Container\Container;
use Illuminate\Queue\InteractsWithQueue;
use ReflectionClass;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\TransportContext;
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

    // Non-null values preserve middleware captured by already queued jobs.
    public mixed $middleware = null;

    public function __construct(
        public readonly Message $message,
    ) {}

    public function middleware(): mixed
    {
        if ($this->middleware !== null) {
            return [];
        }

        $handlerClass = Container::getInstance()->get(SubscriptionRegistry::class)
            ->resolveForQueuedMessage($this->subscriptionName())
            ->handlerClass();

        $handler = new ReflectionClass($handlerClass)->newInstanceWithoutConstructor();

        return method_exists($handler, 'middleware') ? $handler->middleware($this->message) : [];
    }

    public function handle(SubscriptionRegistry $subscriptions, Container $container): void
    {
        $subscription = $subscriptions->resolveForQueuedMessage($this->subscriptionName());
        $handler = $container->get($subscription->handlerClass());

        $handler->handle($this->message);
    }

    public function failed(?Throwable $exception): void
    {
        $container = Container::getInstance();
        $subscriptions = $container->get(SubscriptionRegistry::class);

        try {
            $handlerClass = $subscriptions
                ->resolveForQueuedMessage($this->subscriptionName())
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

    private function subscriptionName(): string
    {
        /** @var TransportContext $transport */
        $transport = $this->message->transport;

        return $transport->subscription;
    }
}
