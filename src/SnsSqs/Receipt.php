<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\SnsSqs;

readonly class Receipt
{
    public function __construct(
        public string $queueUrl,
        public string $handle,
    ) {}
}
