<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use Illuminate\Contracts\Config\Repository;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;

class OwnershipPrefix
{
    public const int MAX_CHARACTERS = 24;

    private const string PATTERN = '/\A[A-Za-z][A-Za-z0-9_-]*\z/';

    public function __construct(
        private readonly Repository $config,
    ) {}

    public function current(): string
    {
        $prefix = $this->config->get('spoolrail.prefix');

        if ($prefix === null || $prefix === '') {
            throw InvalidConfigException::missingOwnershipPrefix();
        }

        return $this->validate($prefix);
    }

    public function validate(mixed $prefix): string
    {
        if (
            ! is_string($prefix)
            || strlen($prefix) > self::MAX_CHARACTERS
            || preg_match(self::PATTERN, $prefix) !== 1
            || str_starts_with(strtolower($prefix), 'goog')
        ) {
            throw InvalidConfigException::invalidOwnershipPrefix();
        }

        return $prefix;
    }

    public function validateFormer(mixed $prefix): string
    {
        if (! is_string($prefix) || preg_match(self::PATTERN, $prefix) !== 1) {
            throw InvalidConfigException::invalidFormerOwnershipPrefix();
        }

        return $prefix;
    }
}
