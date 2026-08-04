<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;
use Closure;
use InvalidArgumentException;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Exceptions\MessageTooLargeException;
use Spoolrail\Spoolrail\Topology\LogicalName;

class Connection
{
    public const int MAX_ENVELOPE_BYTES = 262_144;

    public function __construct(
        private readonly Driver $driver,
        private readonly MessageEnvelope $envelope,
    ) {}

    public function publish(string $topic, Message $message): Message
    {
        if (! LogicalName::isValidTopic($topic)) {
            throw new InvalidArgumentException(
                "Topic [$topic] must contain between 3 and 251 ASCII characters, begin with a letter, otherwise contain only letters, digits, hyphens, and underscores, and avoid transport-reserved beginnings.",
            );
        }

        $stampedMessage = $message->withPublishedAt(CarbonImmutable::now('UTC'));
        $body = $this->envelope->encode($stampedMessage);

        if (strlen($body) > self::MAX_ENVELOPE_BYTES) {
            throw new MessageTooLargeException(strlen($body), self::MAX_ENVELOPE_BYTES);
        }

        $this->driver->publish($topic, $body);

        return $stampedMessage;
    }

    /**
     * @param  Closure(string): void  $handoff
     */
    public function consume(string $subscription, Closure $handoff): void
    {
        if (! LogicalName::isValidSubscription($subscription)) {
            throw new InvalidArgumentException(
                "Subscription [$subscription] must contain between 3 and 50 ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.",
            );
        }

        $this->driver->consume($subscription, $handoff);
    }

    /**
     * @internal
     */
    public function topology(): ?CanManageTopology
    {
        return $this->driver instanceof CanManageTopology ? $this->driver : null;
    }

    /**
     * @internal
     */
    public function close(): void
    {
        if ($this->driver instanceof CanClose) {
            $this->driver->close();
        }
    }
}
