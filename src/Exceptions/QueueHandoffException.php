<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;

class QueueHandoffException extends RuntimeException implements SpoolrailException
{
    public static function alreadyInProgress(string $subscription, string $messageId): self
    {
        return new self("Queue handoff for message [$messageId] on subscription [$subscription] is already in progress.");
    }

    public static function couldNotRetainCompletionLock(string $subscription, string $messageId): self
    {
        return new self("Spoolrail handed message [$messageId] on subscription [$subscription] to Laravel Queue but could not retain its completion lock. The source delivery remains unsettled and may be queued again.");
    }
}
