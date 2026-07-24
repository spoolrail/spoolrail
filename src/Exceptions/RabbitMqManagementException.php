<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Throwable;

class RabbitMqManagementException extends RuntimeException implements SpoolrailException
{
    public static function requestFailed(
        string $connection,
        string $operation,
        Throwable $previous,
    ): self {
        return new self(
            "RabbitMQ connection [$connection] Management API request failed while $operation.",
            previous: $previous,
        );
    }

    public static function unexpectedStatus(
        string $connection,
        string $operation,
        int $status,
    ): self {
        return new self(
            "RabbitMQ connection [$connection] Management API returned HTTP $status while $operation.",
        );
    }

    public static function invalidResponse(string $connection, string $operation): self
    {
        return new self(
            "RabbitMQ connection [$connection] Management API returned an invalid response while $operation.",
        );
    }
}
