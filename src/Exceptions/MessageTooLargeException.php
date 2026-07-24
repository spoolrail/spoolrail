<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use LengthException;

class MessageTooLargeException extends LengthException implements SpoolrailException
{
    public function __construct(int $bytes, int $limit)
    {
        parent::__construct("Serialized message envelope is $bytes bytes; Spoolrail accepts at most $limit bytes.");
    }
}
