<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use LogicException;

class ManagedTopologyUnavailableException extends LogicException implements SpoolrailException
{
    public function __construct(string $connectionName)
    {
        parent::__construct("Spoolrail connection [$connectionName] does not provide package-managed topology.");
    }
}
