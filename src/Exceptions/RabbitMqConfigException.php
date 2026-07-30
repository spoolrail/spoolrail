<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use InvalidArgumentException;

class RabbitMqConfigException extends InvalidArgumentException implements SpoolrailException
{
    public static function invalid(string $connectionName, string $setting, string $requirement): self
    {
        return new self("RabbitMQ connection [$connectionName] setting [$setting] $requirement.");
    }
}
