<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use InvalidArgumentException;
use Spoolrail\Spoolrail\Contracts\MessageHandler;

class InvalidSubscriptionException extends InvalidArgumentException implements SpoolrailException
{
    public static function duplicateName(string $name): self
    {
        return new self("Subscription [$name] has already been registered.");
    }

    public static function notRegistered(string $name): self
    {
        return new self("Subscription [$name] has not been registered.");
    }

    public static function emptyTopic(): self
    {
        return new self('Subscription topic must not be empty.');
    }

    public static function emptyName(): self
    {
        return new self('Subscription name must not be empty.');
    }

    public static function invalidHandler(string $handler): self
    {
        return new self("Subscription handler [$handler] must be a concrete class implementing ".MessageHandler::class.'.');
    }

    public static function emptySetting(string $label): self
    {
        return new self("$label must not be empty.");
    }
}
