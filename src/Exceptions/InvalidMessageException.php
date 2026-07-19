<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use InvalidArgumentException;

class InvalidMessageException extends InvalidArgumentException implements SpoolrailException
{
    public static function emptyType(): self
    {
        return new self('The message type must not be empty.');
    }
}
