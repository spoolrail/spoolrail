<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use LengthException;

class InvalidPhysicalNameException extends LengthException implements SpoolrailException
{
    public function __construct(
        string $logicalName,
        string $physicalName,
        string $prefix,
        int $limit,
    ) {
        parent::__construct(
            "Logical subscription [$logicalName] with ownership prefix [$prefix] derives RabbitMQ queue [$physicalName], which exceeds the $limit-byte transport limit.",
        );
    }
}
