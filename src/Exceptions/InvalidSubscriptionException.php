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

    public static function invalidTopic(string $topic): self
    {
        return new self(
            "Subscription topic [$topic] must contain between 3 and 251 ASCII characters, begin with a letter, otherwise contain only letters, digits, hyphens, and underscores, and avoid transport-reserved beginnings.",
        );
    }

    public static function invalidName(string $name): self
    {
        return new self(
            "Subscription name [$name] must contain between 3 and 50 ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.",
        );
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
