<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;

class RabbitMqConsumerCancelledException extends RuntimeException implements SpoolrailException
{
    public function __construct()
    {
        parent::__construct('RabbitMQ cancelled the consumer unexpectedly.');
    }
}
