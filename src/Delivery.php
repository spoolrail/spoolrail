<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;

/**
 * @template TReceipt
 */
readonly class Delivery
{
    /**
     * @param  TReceipt  $receipt
     * @param  array<string, mixed>  $headers
     */
    public function __construct(
        public string $body,
        public mixed $receipt,
        public array $headers = [],
        public ?string $transportMessageId = null,
        public ?CarbonImmutable $transportPublishedAt = null,
        public ?bool $redelivered = null,
        public ?string $orderingKey = null,
    ) {}
}
