<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Support\Str;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigurationException;

class OwnershipPrefix
{
    private const string PATTERN = '/\A[A-Za-z][A-Za-z0-9_-]*\z/D';

    public function __construct(
        private readonly Repository $config,
    ) {}

    public function value(): string
    {
        $configured = $this->config->get('spoolrail.prefix');

        if ($configured !== null) {
            return $this->validate($configured);
        }

        $applicationName = $this->config->get('app.name');
        $environmentName = $this->config->get('app.env');

        if (! is_string($applicationName) || ! is_string($environmentName)) {
            throw InvalidConfigurationException::invalidOwnershipPrefix();
        }

        $name = Str::slug($applicationName);
        $environment = Str::slug($environmentName);

        return $this->validate("$name-$environment");
    }

    public function validate(mixed $prefix): string
    {
        if (! is_string($prefix) || preg_match(self::PATTERN, $prefix) !== 1) {
            throw InvalidConfigurationException::invalidOwnershipPrefix();
        }

        return $prefix;
    }
}
