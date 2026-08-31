<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Events;

use Spoolrail\Spoolrail\Message;

readonly class MessageConsumed
{
    public function __construct(
        public Message $message,
    ) {}
}
