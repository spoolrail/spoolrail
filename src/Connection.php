<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;
use Closure;
use Spoolrail\Spoolrail\Contracts\ClosableDriver;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Exceptions\InvalidTopicException;
use Spoolrail\Spoolrail\Exceptions\MessageTooLargeException;

class Connection
{
    public const int MAX_ENVELOPE_BYTES = 262_144;

    public function __construct(
        private readonly Driver $driver,
        private readonly MessageSerializer $serializer,
    ) {}

    public function publish(string $topic, Message $message): Message
    {
        if (! LogicalName::isValid($topic)) {
            throw new InvalidTopicException($topic);
        }

        $candidate = $message->withPublishedAt(CarbonImmutable::now('UTC'));
        $body = $this->serializer->serialize($candidate);

        if (strlen($body) > self::MAX_ENVELOPE_BYTES) {
            throw new MessageTooLargeException(strlen($body), self::MAX_ENVELOPE_BYTES);
        }

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

    /**
     * @internal
     */
    public function managedTopology(): ?ManagedTopology
    {
        return $this->driver instanceof ManagedTopology ? $this->driver : null;
    }

    /**
     * @internal
     */
    public function close(): void
    {
        if ($this->driver instanceof ClosableDriver) {
            $this->driver->close();
        }
    }
}
