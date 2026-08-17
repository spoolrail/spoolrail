<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Outbox;

readonly class OutboxLane
{
    public function __construct(
        public int $headId,
        public int $backlogSize,
    ) {}
}
