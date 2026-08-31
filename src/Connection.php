<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;
use Closure;
use Illuminate\Support\Sleep;
use InvalidArgumentException;
use LogicException;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Spoolrail\Spoolrail\Events\MessagePublicationFailed;
use Spoolrail\Spoolrail\Events\MessagePublished;
use Spoolrail\Spoolrail\Events\MessagePublishing;
use Spoolrail\Spoolrail\Events\MessageStaged;
use Spoolrail\Spoolrail\Events\MessageStaging;
use Spoolrail\Spoolrail\Events\MessageStagingFailed;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\MessageTooLargeException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\Outbox\OutboxPublication;
use Spoolrail\Spoolrail\Topology\LogicalName;
use Throwable;

class Connection
{
    public const int MAX_PUBLICATION_BYTES = 262_144;

    private const int MAX_HEADERS = 10;

    private const int MAX_HEADER_KEY_BYTES = 128;

    private const int MAX_HEADER_VALUE_BYTES = 1_024;

    private const int AWS_STRING_DATA_TYPE_BYTES = 6;

    /** @var Driver<covariant mixed>|null */
    private ?Driver $resolvedDriver = null;

    /**
     * @var (Closure(): Driver<covariant mixed>)|null
     */
    private ?Closure $resolveDriver = null;

    /**
     * @param  Driver<covariant mixed>|(Closure(): Driver<covariant mixed>)  $driver
     * @param  (Closure(array<string, string>): mixed)|null  $transformHeadersCallback
     */
    public function __construct(
        Driver|Closure $driver,
        private MessageEnvelope $envelope,
        private string $connectionName = 'default',
        private ?Closure $transformHeadersCallback = null,
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

        $headers = $this->validatedHeaders($headers);
        $this->ensureOrderingKeyIsPortable($orderingKey);

        $stampedMessage = $message->withPublishedAt(CarbonImmutable::now('UTC'));
        $body = $this->envelope->encode($stampedMessage);
        $publicationBytes = $this->publicationBytes($body, $headers);

        if ($publicationBytes > self::MAX_PUBLICATION_BYTES) {
            throw new MessageTooLargeException($publicationBytes, self::MAX_PUBLICATION_BYTES);
        }

        if ($this->outboxEnabled()) {
            $this->publishToOutbox($topic, $stampedMessage, $body, $headers, $orderingKey);

            return $stampedMessage;
        }

        $this->publishDirectly($topic, $stampedMessage, $body, $headers, $orderingKey);

        return $stampedMessage;
    }

    /**
     * @internal
     *
     * @param  array<string, string>  $headers
     */
    public function publishStored(
        string $topic,
        string $body,
        array $headers,
        ?string $orderingKey,
    ): void {
        $message = $this->envelope->decode($body);
        $driver = $this->driver();
        $retries = $this->publisherRetrySetting('times', 2);
        $delayMilliseconds = $this->publisherRetrySetting('delay_milliseconds', 1000);

        $this->dispatch(new MessagePublishing(
            $this->connectionName,
            $topic,
            $message,
            $headers,
            $orderingKey,
        ));

        $this->publishToBroker(
            $driver,
            $retries,
            $delayMilliseconds,
            $topic,
            $message,
            $body,
            $headers,
            $orderingKey,
        );
    }

    /**
     * @param  array<string, string>  $headers
     */
    private function publishToOutbox(
        string $topic,
        Message $message,
        string $body,
        array $headers,
        ?string $orderingKey,
    ): void {
        $this->dispatch(new MessageStaging(
            $this->connectionName,
            $topic,
            $message,
            $headers,
            $orderingKey,
        ));

        $headers = $this->transformPublicationHeaders($body, $headers);

        try {
            $publication = OutboxPublication::query()->create([
                'connection' => $this->connectionName,
                'topic' => $topic,
                'message' => $this->envelope->toArray($message),
                'headers' => $headers,
                'ordering_key' => $orderingKey,
                'last_error' => null,
            ]);
        } catch (Throwable $exception) {
            $this->dispatch(new MessageStagingFailed(
                $this->connectionName,
                $topic,
                $message,
                $headers,
                $orderingKey,
                $exception,
            ));

            throw $exception;
        }

        $this->dispatch(new MessageStaged(
            $this->connectionName,
            $topic,
            $message,
            $headers,
            $orderingKey,
            $publication->id,
        ));
    }

    /**
     * @param  array<string, string>  $headers
     */
    private function publishDirectly(
        string $topic,
        Message $message,
        string $body,
        array $headers,
        ?string $orderingKey,
    ): void {
        $driver = $this->driver();
        $retries = $this->publisherRetrySetting('times', 2);
        $delayMilliseconds = $this->publisherRetrySetting('delay_milliseconds', 1000);

        $this->dispatch(new MessagePublishing(
            $this->connectionName,
            $topic,
            $message,
            $headers,
            $orderingKey,
        ));

        $headers = $this->transformPublicationHeaders($body, $headers);

        $this->publishToBroker(
            $driver,
            $retries,
            $delayMilliseconds,
            $topic,
            $message,
            $body,
            $headers,
            $orderingKey,
        );
    }

    /**
     * @param  Driver<covariant mixed>  $driver
     * @param  array<string, string>  $headers
     */
    private function publishToBroker(
        Driver $driver,
        int $retries,
        int $delayMilliseconds,
        string $topic,
        Message $message,
        string $body,
        array $headers,
        ?string $orderingKey,
    ): void {
        try {
            $this->publishWithRetries(
                $driver,
                $retries,
                $delayMilliseconds,
                $topic,
                $body,
                $headers,
                $orderingKey,
            );
        } catch (Throwable $failure) {
            $failure = $this->asPublicationException($failure);

            $this->dispatch(new MessagePublicationFailed(
                $this->connectionName,
                $topic,
                $message,
                $headers,
                $orderingKey,
                $failure,
            ));

            throw $failure;
        }

        $this->dispatch(new MessagePublished(
            $this->connectionName,
            $topic,
            $message,
            $headers,
            $orderingKey,
        ));
    }

    /**
     * @param  Driver<covariant mixed>  $driver
     * @param  array<string, string>  $headers
     */
    private function publishWithRetries(
        Driver $driver,
        int $retries,
        int $delayMilliseconds,
        string $topic,
        string $body,
        array $headers,
        ?string $orderingKey,
    ): void {
        $unknownFailure = null;

        for ($attempt = 0; ; $attempt++) {
            try {
                $driver->publish($topic, $body, $headers, $orderingKey);

                return;
            } catch (Throwable $failure) {
                $failure = $this->asPublicationException($failure);
                $unknownFailure = $this->latestUnknownFailure($failure, $unknownFailure);

                if ($this->shouldStopPublishing($failure, $attempt, $retries)) {
                    throw $unknownFailure ?? $failure;
                }

                Sleep::for($delayMilliseconds)->milliseconds();
            }
        }
    }

    /**
     * @param  array<string, string>  $headers
     * @return array<string, string>
     */
    private function transformPublicationHeaders(string $body, array $headers): array
    {
        if (! $this->transformHeadersCallback instanceof Closure) {
            return $headers;
        }

        try {
            $candidate = ($this->transformHeadersCallback)($headers);

            if (! is_array($candidate)) {
                return $headers;
            }

            $candidate = $this->validatedHeaders($candidate);

            if ($this->publicationBytes($body, $candidate) > self::MAX_PUBLICATION_BYTES) {
                return $headers;
            }
        } catch (Throwable) {
            return $headers;
        }

        return $candidate;
    }

    private function dispatch(object $event): void
    {
        try {
            event($event);
        } catch (Throwable) {
            // Observers must not enter publication control flow.
        }
    }

    private function asPublicationException(Throwable $failure): PublicationException
    {
        return $failure instanceof PublicationException
            ? $failure
            : PublicationException::outcomeUnknown($failure);
    }

    private function shouldStopPublishing(
        PublicationException $failure,
        int $attempt,
        int $retries,
    ): bool {
        return $failure->outcome === PublicationOutcome::Rejected || $attempt >= $retries;
    }

    private function latestUnknownFailure(
        PublicationException $failure,
        ?PublicationException $previousUnknown,
    ): ?PublicationException {
        return $failure->outcome === PublicationOutcome::Unknown ? $failure : $previousUnknown;
    }

    private function publisherRetrySetting(string $setting, int $default): int
    {
        $value = config("spoolrail.publisher_retries.$setting", $default);

        if (! is_int($value) || $value < 0) {
            throw InvalidConfigException::invalidPublisherRetrySetting($setting);
        }

        return $value;
    }

    /**
     * @param  array<array-key, mixed>  $headers
     * @return array<string, string>
     */
    private function validatedHeaders(array $headers): array
    {
        if (count($headers) > self::MAX_HEADERS) {
            throw new InvalidArgumentException(
                'A message publication may contain at most 10 headers.',
            );
        }

        $validated = [];

        foreach ($headers as $key => $value) {
            $key = $this->ensureHeaderKeyIsPortable($key);
            $validated[$key] = $this->ensureHeaderValueIsPortable($key, $value);
        }

        return $validated;
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

    private function ensureHeaderValueIsPortable(string $key, mixed $value): string
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

        return $value;
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

    /**
     * @internal
     *
     * @return Driver<covariant mixed>
     */
    public function consumerDriver(): Driver
    {
        return $this->driver();
    }

    /**
     * @return Driver<covariant mixed>
     */
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
