<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;

class OutboxProcessException extends RuntimeException implements SpoolrailException
{
    public static function unsupportedRuntime(string $reason): self
    {
        return new self("Spoolrail cannot run concurrent outbox workers because $reason");
    }
}
