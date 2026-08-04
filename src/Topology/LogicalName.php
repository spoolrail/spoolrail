<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

class LogicalName
{
    public const string PATTERN = '/\A[A-Za-z][A-Za-z0-9_-]{2,}\z/';

    public const int MAX_TOPIC_CHARACTERS = 251;

    public const int MAX_SUBSCRIPTION_CHARACTERS = 50;

    public static function isValidTopic(string $value): bool
    {
        return self::matchesGrammarWithin($value, self::MAX_TOPIC_CHARACTERS)
            && ! self::usesReservedBeginning($value);
    }

    public static function isValidSubscription(string $value): bool
    {
        return self::matchesGrammarWithin($value, self::MAX_SUBSCRIPTION_CHARACTERS);
    }

    private static function matchesGrammarWithin(string $value, int $maximumCharacters): bool
    {
        return strlen($value) <= $maximumCharacters
            && preg_match(self::PATTERN, $value) === 1;
    }

    private static function usesReservedBeginning(string $value): bool
    {
        return str_starts_with(strtolower($value), 'goog');
    }
}
