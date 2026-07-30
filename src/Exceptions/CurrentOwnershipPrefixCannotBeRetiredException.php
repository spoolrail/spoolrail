<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use InvalidArgumentException;

class CurrentOwnershipPrefixCannotBeRetiredException extends InvalidArgumentException implements SpoolrailException
{
    public function __construct(string $prefix)
    {
        parent::__construct("Ownership prefix [$prefix] is current and cannot be supplied as a retired prefix.");
    }
}
