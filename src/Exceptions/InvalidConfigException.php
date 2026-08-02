<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use InvalidArgumentException;

class InvalidConfigException extends InvalidArgumentException implements SpoolrailException
{
    public static function invalidDefaultConnection(): self
    {
        return new self('Spoolrail default connection must be a non-empty string.');
    }

    public static function undefinedConnection(string $connectionName): self
    {
        return new self("Spoolrail connection [$connectionName] is not defined.");
    }

    public static function connectionMustBeArray(string $connectionName): self
    {
        return new self("Spoolrail connection [$connectionName] config must be an array.");
    }

    public static function missingDriver(string $connectionName): self
    {
        return new self("Spoolrail connection [$connectionName] must define a non-empty string [driver].");
    }

    public static function unsupportedDriver(string $driver): self
    {
        return new self("Spoolrail driver [$driver] is not supported.");
    }

    public static function invalidOwnershipPrefix(): self
    {
        return new self('Spoolrail ownership prefix must begin with an ASCII letter and contain only ASCII letters, digits, hyphens, and underscores.');
    }

    public static function rabbitMqSetting(string $connectionName, string $setting, string $requirement): self
    {
        return new self("RabbitMQ connection [$connectionName] setting [$setting] $requirement.");
    }

    public static function deduplicationStore(?string $store): self
    {
        $name = $store ?? 'default';

        return new self("Spoolrail deduplication requires a cache store that supports atomic locks, and the [$name] cache store does not. Configure [spoolrail.deduplication.store] with a lock-capable store or disable [spoolrail.deduplication.enabled].");
    }
}
