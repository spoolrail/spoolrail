<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

class RabbitMqManagementConfig
{
    public function __construct(
        public readonly string $url,
        public readonly string $username,
        public readonly string $password,
        public readonly ?string $caFile,
    ) {}
}
