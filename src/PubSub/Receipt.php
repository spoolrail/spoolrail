<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\PubSub;

readonly class Receipt
{
    public function __construct(
        public string $subscriptionPath,
        public string $ackId,
    ) {}
}
