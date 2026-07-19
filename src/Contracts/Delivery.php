<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

interface Delivery
{
    public function body(): string;

    public function acknowledge(): void;
}
