<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;

class RabbitMqManagementException extends RuntimeException implements SpoolrailException
{
    public static function requestFailed(
        string $connectionName,
        string $operation,
        string $reason,
    ): self {
        return new self(
            "RabbitMQ connection [$connectionName] Management API request failed while $operation.",
            previous: new RuntimeException($reason),
        );
    }

    public static function unexpectedStatus(
        string $connectionName,
        string $operation,
        int $status,
    ): self {
        return new self(
            "RabbitMQ connection [$connectionName] Management API returned HTTP $status while $operation.",
        );
    }

    public static function invalidResponse(string $connectionName, string $operation): self
    {
        return new self(
            "RabbitMQ connection [$connectionName] Management API returned an invalid response while $operation.",
        );
    }
}
