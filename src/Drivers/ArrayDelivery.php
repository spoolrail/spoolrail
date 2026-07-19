<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Drivers;

use Spoolrail\Spoolrail\Contracts\Delivery;

class ArrayDelivery implements Delivery
{
    private bool $isAcknowledged = false;

    public function __construct(private readonly string $messageBody) {}

    public function body(): string
    {
        return $this->messageBody;
    }

    public function acknowledge(): void
    {
        $this->isAcknowledged = true;
    }

    public function isAcknowledged(): bool
    {
        return $this->isAcknowledged;
    }
}
