<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use InvalidArgumentException;

class RabbitMqConfigurationException extends InvalidArgumentException implements SpoolrailException
{
    public static function invalid(string $connection, string $setting, string $requirement): self
    {
        return new self("RabbitMQ connection [$connection] setting [$setting] $requirement.");
    }

    public static function missingManagementUrl(string $connection): self
    {
        return new self(
            "RabbitMQ connection [$connection] must configure [management.url] before running topology commands.",
        );
    }

    public static function incompleteManagementCredentials(string $connection): self
    {
        return new self(
            "RabbitMQ connection [$connection] must configure both [management.username] and [management.password], or neither to reuse the AMQP URI credentials.",
        );
    }
}
