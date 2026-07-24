<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use LengthException;

class InvalidRabbitMqTopicNameException extends LengthException implements SpoolrailException
{
    public function __construct(string $topic, int $limit)
    {
        parent::__construct(
            "Logical topic [$topic] is also its RabbitMQ exchange name and exceeds the $limit-byte transport limit.",
        );
    }
}
