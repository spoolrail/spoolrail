<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;

class RabbitMqPublicationRejectedException extends RuntimeException implements SpoolrailException
{
    public function __construct()
    {
        parent::__construct('RabbitMQ rejected the publication.');
    }
}
