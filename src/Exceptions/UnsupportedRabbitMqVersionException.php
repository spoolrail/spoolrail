<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqVersion;

class UnsupportedRabbitMqVersionException extends RuntimeException implements SpoolrailException
{
    public function __construct(string $version)
    {
        parent::__construct("RabbitMQ [$version] is not supported; Spoolrail requires RabbitMQ ".RabbitMqVersion::MINIMUM.' or later.');
    }
}
