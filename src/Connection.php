<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;
use Closure;
use Spoolrail\Spoolrail\Contracts\Driver;

class Connection
{
    public function __construct(
        private readonly Driver $driver,
        private readonly MessageSerializer $serializer,
    ) {}

    public function publish(string $topic, Message $message): Message
    {
        $candidate = $message->withPublishedAt(CarbonImmutable::now('UTC'));
        $body = $this->serializer->serialize($candidate);

        $this->driver->publish($topic, $body);

        return $candidate;
    }

    /**
     * @param  Closure(string): void  $handoff
     */
    public function consume(string $subscription, Closure $handoff): void
    {
        $this->driver->consume($subscription, $handoff);
    }
}
