<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Throwable;

class PublicationException extends RuntimeException implements SpoolrailException
{
    private function __construct(
        public readonly PublicationOutcome $outcome,
        string $message,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, previous: $previous);
    }

    public static function notSent(Throwable $previous): self
    {
        return new self(
            PublicationOutcome::NotSent,
            'The RabbitMQ publication failed before the message was sent.',
            $previous,
        );
    }

    public static function rejected(): self
    {
        return new self(
            PublicationOutcome::Rejected,
            'RabbitMQ rejected the publication.',
        );
    }

    public static function outcomeUnknown(Throwable $previous): self
    {
        return new self(
            PublicationOutcome::Unknown,
            'RabbitMQ did not confirm the publication; the message may have been accepted.',
            $previous,
        );
    }
}
