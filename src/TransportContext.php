<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;

readonly class TransportContext
{
    /**
     * @param  array<string, mixed>  $headers
     */
    public function __construct(
        public string $driver,
        public string $connectionName,
        public string $topic,
        public string $subscription,
        public array $headers,
        public ?string $transportMessageId = null,
        public ?CarbonImmutable $transportPublishedAt = null,
        public ?bool $redelivered = null,
        public ?string $orderingKey = null,
    ) {}
}
