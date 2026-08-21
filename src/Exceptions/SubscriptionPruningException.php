<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;

class SubscriptionPruningException extends RuntimeException implements SpoolrailException
{
    public static function noDeclaredSubscriptions(string $connectionName): self
    {
        return new self(
            "Spoolrail connection [$connectionName] has no declared subscriptions, so pruning its current ownership prefix was refused. Load the expected subscription declarations, or use [--retired-prefix] to delete a former prefix completely.",
        );
    }
}
