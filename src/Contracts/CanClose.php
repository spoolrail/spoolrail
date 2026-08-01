<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

interface CanClose
{
    public function close(): void;
}
