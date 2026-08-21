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
use Spoolrail\Spoolrail\Testing\Fakes\SpoolrailFake;

/**
 * @method static Connection connection(?string $name = null)
 * @method static SpoolrailManager extend(string $driver, Closure $creator)
 * @method static void forgetConnection(?string $name = null)
 * @method static Message publish(string $topic, Message $message, array<string, string> $headers = [], ?string $orderingKey = null)
 * @method static void assertNothingPublished()
 * @method static void assertNotPublished(string $topic, string $type, ?Closure $callback = null)
 * @method static void assertPublished(string $topic, string $type, Closure|int|null $callback = null)
 *
 * @see SpoolrailManager
 * @see SpoolrailFake
 */
class Spoolrail extends Facade
{
    public static function fake(): SpoolrailFake
    {
        $fake = new SpoolrailFake;

        static::swap($fake);

        return $fake;
    }

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
