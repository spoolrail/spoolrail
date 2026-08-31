<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Events;

use Spoolrail\Spoolrail\Message;
use Throwable;

readonly class MessageConsumptionFailed
{
    public function __construct(
        public Message $message,
        public Throwable $exception,
    ) {}
}
