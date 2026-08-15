<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\PubSub;

use Carbon\CarbonImmutable;
use Google\Cloud\PubSub\Message;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use UnexpectedValueException;

class Delivery
{
    /**
     * @param  array<string, mixed>  $headers
     */
    private function __construct(
        public readonly Message $message,
        public readonly string $body,
        private array $headers,
        private ?string $messageId,
        private ?CarbonImmutable $publishedAt,
        private ?bool $redelivered,
        private ?string $orderingKey,
    ) {}

    public static function fromMessage(Message $message): self
    {
        $body = $message->data();
        $ackId = $message->ackId();

        self::ensureDeliveryIsUsable($body, $ackId);

        $publishedAt = $message->publishTime();
        $deliveryAttempt = $message->deliveryAttempt();

        return new self(
            $message,
            $body,
            self::headersFrom($message),
            $message->id(),
            $publishedAt === null ? null : CarbonImmutable::instance($publishedAt),
            $deliveryAttempt === null ? null : $deliveryAttempt > 1,
            $message->orderingKey(),
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function headers(): array
    {
        return $this->headers;
    }

    public function transportMessageId(): ?string
    {
        return $this->messageId;
    }

    public function publishedAt(): ?CarbonImmutable
    {
        return $this->publishedAt;
    }

    public function wasRedelivered(): ?bool
    {
        return $this->redelivered;
    }

    public function orderingKey(): ?string
    {
        return $this->orderingKey;
    }

    private static function ensureDeliveryIsUsable(mixed $body, mixed $ackId): void
    {
        if (! is_string($body) || ! is_string($ackId) || $ackId === '') {
            throw ConsumptionException::consumerStopped(
                new UnexpectedValueException('Google Pub/Sub returned an invalid message delivery.'),
            );
        }
    }

    /**
     * @return array<string, mixed>
     */
    private static function headersFrom(Message $message): array
    {
        return array_filter(
            $message->attributes(),
            static fn (mixed $value, mixed $key): bool => is_string($key),
            ARRAY_FILTER_USE_BOTH,
        );
    }
}
