<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Facades;

use Closure;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\Facades\Facade;
use Override;
use ReflectionException;
use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

/**
 * @method static Connection connection(?string $name = null)
 * @method static SpoolrailManager extend(string $driver, Closure $creator)
 * @method static void forgetConnection(?string $name = null)
 * @method static Message publish(string $topic, Message $message, array<string, string> $headers = [])
 *
 * @see SpoolrailManager
 */
class Spoolrail extends Facade
{
    /**
     * @param  class-string<MessageHandler>  $handler
     *
     * @throws BindingResolutionException
     * @throws InvalidSubscriptionException
     * @throws ReflectionException
     */
    public static function subscribe(string $topic, string $name, string $handler): Subscription
    {
        /** @var Application $app */
        $app = static::getFacadeApplication();

        return $app
            ->make(SubscriptionRegistry::class)
            ->subscribe($topic, $name, $handler);
    }

    #[Override]
    protected static function getFacadeAccessor(): string
    {
        return 'spoolrail';
    }
}
