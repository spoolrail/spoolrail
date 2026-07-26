<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Illuminate\Contracts\Config\Repository;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigurationException;

class OwnershipPrefix
{
    private const string PATTERN = '/\A[A-Za-z][A-Za-z0-9_-]*\z/';

    public function __construct(
        private readonly Repository $config,
    ) {}

    public function value(): string
    {
        return $this->validate($this->config->get('spoolrail.prefix'));
    }

    public function validate(mixed $prefix): string
    {
        if (! is_string($prefix) || preg_match(self::PATTERN, $prefix) !== 1) {
            throw InvalidConfigurationException::invalidOwnershipPrefix();
        }

        return $prefix;
    }
}
