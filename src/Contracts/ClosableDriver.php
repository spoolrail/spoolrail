<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

interface ClosableDriver
{
    public function close(): void;
}
