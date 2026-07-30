<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Throwable;

class RabbitMqManagementException extends RuntimeException implements SpoolrailException
{
    private function __construct(
        public readonly string $connectionName,
        public readonly string $operation,
        public readonly ?int $status,
        string $message,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, previous: $previous);
    }

    public static function requestFailed(
        string $connectionName,
        string $operation,
        Throwable $previous,
    ): self {
        return new self(
            $connectionName,
            $operation,
            null,
            "RabbitMQ connection [$connectionName] Management API request failed while $operation.",
            $previous,
        );
    }

    public static function unexpectedStatus(
        string $connectionName,
        string $operation,
        int $status,
    ): self {
        return new self(
            $connectionName,
            $operation,
            $status,
            "RabbitMQ connection [$connectionName] Management API returned HTTP $status while $operation.",
        );
    }

    public static function invalidResponse(string $connectionName, string $operation): self
    {
        return new self(
            $connectionName,
            $operation,
            null,
            "RabbitMQ connection [$connectionName] Management API returned an invalid response while $operation.",
        );
    }
}
