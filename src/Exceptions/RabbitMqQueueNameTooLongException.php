<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use LengthException;

class RabbitMqQueueNameTooLongException extends LengthException implements SpoolrailException
{
    public function __construct(
        string $subscriptionName,
        string $queueName,
        string $ownershipPrefix,
        int $limit,
    ) {
        parent::__construct(
            "Logical subscription [$subscriptionName] with ownership prefix [$ownershipPrefix] derives RabbitMQ queue [$queueName], which exceeds the $limit-byte transport limit.",
        );
    }
}
