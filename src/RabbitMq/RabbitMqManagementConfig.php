<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

readonly class RabbitMqManagementConfig
{
    public function __construct(
        public string $url,
        public string $username,
        public string $password,
        public ?string $caFile,
    ) {}
}
