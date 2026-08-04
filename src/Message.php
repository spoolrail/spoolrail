<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;
use InvalidArgumentException;
use Ramsey\Uuid\Uuid;

readonly class Message
{
    private const int MAX_TYPE_BYTES = 255;

    /**
     * @param  array<array-key, mixed>  $payload
     */
    private function __construct(
        public string $id,
        public string $type,
        public array $payload,
        public ?CarbonImmutable $publishedAt,
        public ?TransportContext $transport,
    ) {}

    /**
     * @param  array<array-key, mixed>  $payload
     */
    public static function make(string $type, array $payload): self
    {
        if (! self::isValidType($type)) {
            throw new InvalidArgumentException(
                'The message type must be a non-empty valid UTF-8 string of at most 255 bytes.',
            );
        }

        return new self(
            id: Uuid::uuid7()->toString(),
            type: $type,
            payload: $payload,
            publishedAt: null,
            transport: null,
        );
    }

    /**
     * @internal
     */
    public static function isValidType(string $type): bool
    {
        return trim($type) !== ''
            && preg_match('//u', $type) === 1
            && strlen($type) <= self::MAX_TYPE_BYTES;
    }

    /**
     * @internal
     *
     * @param  array<array-key, mixed>  $payload
     */
    public static function fromEnvelope(
        string $id,
        string $type,
        array $payload,
        CarbonImmutable $publishedAt,
    ): self {
        return new self(
            id: $id,
            type: $type,
            payload: $payload,
            publishedAt: $publishedAt,
            transport: null,
        );
    }

    /**
     * @internal
     */
    public function withPublishedAt(CarbonImmutable $publishedAt): self
    {
        return new self(
            id: $this->id,
            type: $this->type,
            payload: $this->payload,
            publishedAt: $publishedAt->utc()->startOfMillisecond(),
            transport: null,
        );
    }

    /**
     * @internal
     */
    public function withTransport(TransportContext $transport): self
    {
        return new self(
            id: $this->id,
            type: $this->type,
            payload: $this->payload,
            publishedAt: $this->publishedAt,
            transport: $transport,
        );
    }
}
