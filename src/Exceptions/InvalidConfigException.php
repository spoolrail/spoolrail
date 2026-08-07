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

    public static function missingOwnershipPrefix(): self
    {
        return new self('Spoolrail ownership prefix is required for receive-side operations. Set [SPOOLRAIL_PREFIX] to a stable application identifier.');
    }

    public static function invalidOwnershipPrefix(): self
    {
        return new self('Spoolrail ownership prefix must contain at most 24 ASCII characters, begin with a letter, otherwise contain only letters, digits, hyphens, and underscores, and avoid transport-reserved beginnings.');
    }

    public static function invalidFormerOwnershipPrefix(): self
    {
        return new self('Former Spoolrail ownership prefix must begin with an ASCII letter and contain only ASCII letters, digits, hyphens, and underscores.');
    }

    public static function rabbitMqSetting(string $connectionName, string $setting, string $requirement): self
    {
        return new self("RabbitMQ connection [$connectionName] setting [$setting] $requirement.");
    }

    public static function unsupportedHandoffIdempotencyCacheStore(?string $store): self
    {
        $name = $store ?? 'default';

        return new self("Spoolrail Queue handoff idempotency requires a cache store backed by Laravel atomic locks, and the [$name] cache store is not supported. Configure [spoolrail.handoff_idempotency.cache_store] with a supported store.");
    }

    public static function invalidHandoffIdempotencyExpiry(): self
    {
        return new self('Spoolrail Queue handoff idempotency expiry must be a positive integer.');
    }

    public static function invalidOutboxConnection(): self
    {
        return new self('Spoolrail outbox connection must be null or a non-empty Laravel database connection name.');
    }

    public static function invalidOutboxExceptionCooldown(): self
    {
        return new self('Spoolrail outbox exception cooldown must be a positive integer.');
    }
}
