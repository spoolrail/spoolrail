<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

interface Driver
{
    public function publish(string $topic, string $body): void;
}
