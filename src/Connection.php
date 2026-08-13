<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;
use Closure;
use InvalidArgumentException;
use LogicException;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Exceptions\MessageTooLargeException;
use Spoolrail\Spoolrail\Outbox\OutboxPublication;
use Spoolrail\Spoolrail\Topology\LogicalName;

class Connection
{
    public const int MAX_PUBLICATION_BYTES = 262_144;

    private const int MAX_HEADERS = 10;

    private const int MAX_HEADER_KEY_BYTES = 128;

    private const int MAX_HEADER_VALUE_BYTES = 1_024;

    private const int AWS_STRING_DATA_TYPE_BYTES = 6;

    private ?Driver $resolvedDriver = null;

    /**
     * @var (Closure(): Driver)|null
     */
    private ?Closure $resolveDriver = null;

    /**
     * @param  Driver|(Closure(): Driver)  $driver
     */
    public function __construct(
        Driver|Closure $driver,
        private MessageEnvelope $envelope,
        private string $connectionName = 'default',
    ) {
        if ($driver instanceof Driver) {
            $this->resolvedDriver = $driver;
        } else {
            $this->resolveDriver = $driver;
        }
    }

    /**
     * @param  array<string, string>  $headers
     */
    public function publish(
        string $topic,
        Message $message,
        array $headers = [],
        ?string $orderingKey = null,
    ): Message {
        if (! LogicalName::isValidTopic($topic)) {
            throw new InvalidArgumentException(
                "Topic [$topic] must contain between 3 and 251 ASCII characters, begin with a letter, otherwise contain only letters, digits, hyphens, and underscores, and avoid transport-reserved beginnings.",
            );
        }

        $this->ensureHeadersArePortable($headers);
        $this->ensureOrderingKeyIsPortable($orderingKey);

        $stampedMessage = $message->withPublishedAt(CarbonImmutable::now('UTC'));
        $body = $this->envelope->encode($stampedMessage);
        $publicationBytes = $this->publicationBytes($body, $headers);

        if ($publicationBytes > self::MAX_PUBLICATION_BYTES) {
            throw new MessageTooLargeException($publicationBytes, self::MAX_PUBLICATION_BYTES);
        }

        if ($this->outboxEnabled()) {
            OutboxPublication::query()->create([
                'connection' => $this->connectionName,
                'topic' => $topic,
                'message' => $this->envelope->toArray($stampedMessage),
                'headers' => $headers,
                'ordering_key' => $orderingKey,
                'last_error' => null,
            ]);
        } else {
            $this->driver()->publish($topic, $body, $headers, $orderingKey);
        }

        return $stampedMessage;
    }

    /**
     * @internal
     *
     * @param  array<string, string>  $headers
     */
    public function publishStored(
        string $topic,
        string $message,
        array $headers,
        ?string $orderingKey,
    ): void {
        $this->driver()->publish($topic, $message, $headers, $orderingKey);
    }

    /**
     * @param  Closure(string, TransportContext): void  $handoff
     */
    public function consume(string $subscription, Closure $handoff): void
    {
        if (! LogicalName::isValidSubscription($subscription)) {
            throw new InvalidArgumentException(
                "Subscription [$subscription] must contain between 3 and 50 ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.",
            );
        }

        $this->driver()->consume($subscription, $handoff);
    }

    /**
     * @internal
     */
    public function ensureConfigured(): void
    {
        $this->driver();
    }

    /**
     * @param  array<array-key, mixed>  $headers
     */
    private function ensureHeadersArePortable(array $headers): void
    {
        if (count($headers) > self::MAX_HEADERS) {
            throw new InvalidArgumentException(
                'A message publication may contain at most 10 headers.',
            );
        }

        foreach ($headers as $key => $value) {
            $key = $this->ensureHeaderKeyIsPortable($key);
            $this->ensureHeaderValueIsPortable($key, $value);
        }
    }

    private function ensureHeaderKeyIsPortable(int|string $key): string
    {
        if (
            ! is_string($key)
            || preg_match('/\A[a-z][a-z0-9]*(?:-[a-z0-9]+)*\z/', $key) !== 1
        ) {
            throw new InvalidArgumentException(
                'Message header keys must begin with a lowercase letter and contain only lowercase letters, digits, and single hyphens between non-empty segments.',
            );
        }

        if (strlen($key) > self::MAX_HEADER_KEY_BYTES) {
            throw new InvalidArgumentException(
                "Message header [$key] exceeds the 128-byte limit.",
            );
        }

        return $key;
    }

    private function ensureHeaderValueIsPortable(string $key, mixed $value): void
    {
        if (! is_string($value)) {
            throw new InvalidArgumentException(
                "Message header [$key] must have a string value.",
            );
        }

        if (preg_match('/./su', $value) !== 1) {
            throw new InvalidArgumentException(
                "Message header [$key] must have a non-empty valid UTF-8 value.",
            );
        }

        if (strlen($value) > self::MAX_HEADER_VALUE_BYTES) {
            throw new InvalidArgumentException(
                "Message header [$key] exceeds the 1024-byte value limit.",
            );
        }
    }

    private function ensureOrderingKeyIsPortable(?string $orderingKey): void
    {
        if ($orderingKey === null) {
            return;
        }

        if (preg_match('/\A[\x21-\x7E]{1,128}\z/', $orderingKey) !== 1) {
            throw new InvalidArgumentException(
                'The ordering key must contain between 1 and 128 printable ASCII characters without spaces.',
            );
        }
    }

    /**
     * @param  array<string, string>  $headers
     */
    private function publicationBytes(string $body, array $headers): int
    {
        $bytes = strlen($body);

        foreach ($headers as $key => $value) {
            $bytes += strlen($key) + strlen($value) + self::AWS_STRING_DATA_TYPE_BYTES;
        }

        return $bytes;
    }

    /**
     * @internal
     */
    public function topology(): ?CanManageTopology
    {
        $driver = $this->driver();

        return $driver instanceof CanManageTopology ? $driver : null;
    }

    /**
     * @internal
     */
    public function close(): void
    {
        if ($this->resolvedDriver instanceof CanClose) {
            $this->resolvedDriver->close();
        }
    }

    private function driver(): Driver
    {
        if ($this->resolvedDriver instanceof Driver) {
            return $this->resolvedDriver;
        }

        if (! $this->resolveDriver instanceof Closure) {
            throw new LogicException('The Spoolrail connection has no driver resolver.');
        }

        return $this->resolvedDriver = ($this->resolveDriver)();
    }

    private function outboxEnabled(): bool
    {
        return config('spoolrail.outbox.enabled', false) === true;
    }
}
