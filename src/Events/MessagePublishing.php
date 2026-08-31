<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Events;

use Spoolrail\Spoolrail\Message;

readonly class MessagePublishing
{
    /**
     * @param  array<string, string>  $headers
     */
    public function __construct(
        public string $connectionName,
        public string $topic,
        public Message $message,
        public array $headers,
        public ?string $orderingKey,
    ) {}
}
