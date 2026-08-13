<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Throwable;

class TopologySyncRequiresRetryException extends RuntimeException implements SpoolrailException
{
    public static function afterFailure(Throwable $previous): self
    {
        return new self(
            'Topology synchronization must be retried after a retryable broker failure.',
            previous: $previous,
        );
    }
}
