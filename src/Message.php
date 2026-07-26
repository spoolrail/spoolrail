<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;
use Ramsey\Uuid\Uuid;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageException;

readonly class Message
{
    /**
     * @param  array<array-key, mixed>  $payload
     */
    private function __construct(
        public string $id,
        public string $type,
        public array $payload,
        public ?CarbonImmutable $publishedAt,
    ) {}

    /**
     * @param  array<array-key, mixed>  $payload
     */
    public static function make(string $type, array $payload): self
    {
        if (trim($type) === '') {
            throw InvalidMessageException::emptyType();
        }

        return new self(
            id: Uuid::uuid7()->toString(),
            type: $type,
            payload: $payload,
            publishedAt: null,
        );
    }

    /**
     * @internal
     *
     * @param  array<array-key, mixed>  $payload
     */
    public static function fromTransport(
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
        );
    }
}
