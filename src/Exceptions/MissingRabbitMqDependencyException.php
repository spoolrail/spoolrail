<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use LogicException;

class MissingRabbitMqDependencyException extends LogicException implements SpoolrailException
{
    public function __construct()
    {
        parent::__construct(
            'The RabbitMQ driver requires php-amqplib/php-amqplib:^3.7.4. Install it in the application before selecting this driver.',
        );
    }
}
