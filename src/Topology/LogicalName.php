<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

class LogicalName
{
    public const string PATTERN = '/\A[A-Za-z][A-Za-z0-9_-]{2,}\z/';

    public static function isValid(string $value): bool
    {
        return preg_match(self::PATTERN, $value) === 1;
    }
}
