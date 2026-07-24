<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Throwable;

class TopologyPreflightException extends RuntimeException implements SpoolrailException
{
    /**
     * @param  array<string, Throwable>  $failures
     */
    public function __construct(public readonly array $failures)
    {
        $details = array_map(
            static fn (Throwable $failure, string $connection): string => "[$connection] {$failure->getMessage()}",
            $failures,
            array_keys($failures),
        );

        parent::__construct("Spoolrail topology preflight failed:\n- ".implode("\n- ", $details));
    }
}
