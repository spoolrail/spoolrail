<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

class LogicalName
{
    public const PATTERN = '/\A[A-Za-z][A-Za-z0-9_-]{2,}\z/D';

    public static function isValid(string $value): bool
    {
        return preg_match(self::PATTERN, $value) === 1;
    }
}
