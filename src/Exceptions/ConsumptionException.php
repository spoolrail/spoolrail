<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Spoolrail\Spoolrail\Enums\ConsumptionFailure;
use Throwable;

class ConsumptionException extends RuntimeException implements SpoolrailException
{
    private function __construct(
        public readonly ConsumptionFailure $failure,
        string $message,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, previous: $previous);
    }

    public static function consumerStopped(?Throwable $previous = null): self
    {
        return new self(
            ConsumptionFailure::ConsumerStopped,
            'Transport consumption stopped unexpectedly.',
            $previous,
        );
    }

    public static function settlementFailed(Throwable $previous): self
    {
        return new self(
            ConsumptionFailure::SettlementFailed,
            'The transport could not settle a message after handing it to Laravel Queue.',
            $previous,
        );
    }
}
