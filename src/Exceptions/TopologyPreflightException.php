<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Throwable;

class TopologyPreflightException extends RuntimeException implements SpoolrailException
{
    /**
     * @param  array<string, Throwable>  $failuresByConnection
     */
    public function __construct(public readonly array $failuresByConnection)
    {
        $details = array_map(
            static fn (Throwable $failure, string $connectionName): string => "[$connectionName] {$failure->getMessage()}",
            $failuresByConnection,
            array_keys($failuresByConnection),
        );

        parent::__construct("Spoolrail topology preflight failed:\n- ".implode("\n- ", $details));
    }
}
