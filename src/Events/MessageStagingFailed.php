<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Events;

use Spoolrail\Spoolrail\Message;
use Throwable;

readonly class MessageStagingFailed
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
        public Throwable $exception,
    ) {}
}
