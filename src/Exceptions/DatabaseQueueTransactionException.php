<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use LogicException;

class DatabaseQueueTransactionException extends LogicException implements SpoolrailException
{
    public function __construct()
    {
        parent::__construct(
            "Laravel's database Queue cannot accept a Spoolrail handoff while its connection has an open transaction. Commit or roll back that transaction before consuming, or use another Queue connection.",
        );
    }
}
