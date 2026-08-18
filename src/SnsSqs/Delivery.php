<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\SnsSqs;

use Carbon\CarbonImmutable;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use UnexpectedValueException;

class Delivery
{
    /**
     * @param  array<array-key, mixed>  $attributes
     * @param  array<array-key, mixed>  $messageAttributes
     */
    private function __construct(
        public readonly string $body,
        public readonly string $receiptHandle,
        private array $attributes,
        private array $messageAttributes,
        private ?string $messageId,
    ) {}

    /**
     * @return list<self>
     */
    public static function fromMessages(mixed $messages): array
    {
        if (! is_array($messages) || $messages === []) {
            return [];
        }

        $deliveries = [];

        foreach ($messages as $message) {
            if (! is_array($message)) {
                throw self::invalid('SQS returned an invalid message delivery.');
            }

            $deliveries[] = new self(
                self::requiredString($message, 'Body'),
                self::requiredString($message, 'ReceiptHandle'),
                self::map($message, 'Attributes'),
                self::map($message, 'MessageAttributes'),
                self::optionalString($message, 'MessageId'),
            );
        }

        return $deliveries;
    }

    public function transportMessageId(): ?string
    {
        return $this->messageId;
    }

    public function publishedAt(): ?CarbonImmutable
    {
        $timestamp = $this->integerAttribute('SentTimestamp');

        if ($timestamp === null) {
            return null;
        }

        return CarbonImmutable::createFromTimestampMsUTC($timestamp);
    }

    public function wasRedelivered(): ?bool
    {
        $receiveCount = $this->integerAttribute('ApproximateReceiveCount');

        if ($receiveCount === null) {
            return null;
        }

        return $receiveCount > 1;
    }

    public function orderingKey(): ?string
    {
        $messageGroup = $this->attributes['MessageGroupId'] ?? null;

        return is_string($messageGroup) ? $messageGroup : null;
    }

    /**
     * @return array<string, mixed>
     */
    public function headers(): array
    {
        $headers = [];

        foreach ($this->messageAttributes as $name => $attribute) {
            if (! is_string($name)) {
                continue;
            }
            if (! is_array($attribute)) {
                continue;
            }
            $headers[$name] = $this->headerValue($attribute);
        }

        return $headers;
    }

    /**
     * @param  array<mixed, mixed>  $attribute
     */
    private function headerValue(array $attribute): mixed
    {
        if (array_key_exists('StringValue', $attribute)) {
            return $attribute['StringValue'];
        }

        if (array_key_exists('BinaryValue', $attribute)) {
            return $attribute['BinaryValue'];
        }

        return $attribute;
    }

    private function integerAttribute(string $name): ?int
    {
        $value = $this->attributes[$name] ?? null;

        if (! is_string($value)) {
            return null;
        }

        return ctype_digit($value) ? (int) $value : null;
    }

    /**
     * @param  array<mixed, mixed>  $delivery
     */
    private static function requiredString(array $delivery, string $key): string
    {
        $value = $delivery[$key] ?? null;

        if (! is_string($value) || $value === '') {
            throw self::invalid('SQS returned a delivery without a body or receipt handle.');
        }

        return $value;
    }

    /**
     * @param  array<mixed, mixed>  $delivery
     * @return array<array-key, mixed>
     */
    private static function map(array $delivery, string $key): array
    {
        $value = $delivery[$key] ?? null;

        return is_array($value) ? $value : [];
    }

    /**
     * @param  array<mixed, mixed>  $delivery
     */
    private static function optionalString(array $delivery, string $key): ?string
    {
        $value = $delivery[$key] ?? null;

        return is_string($value) ? $value : null;
    }

    private static function invalid(string $message): ConsumptionException
    {
        return ConsumptionException::consumerStopped(
            new UnexpectedValueException($message),
        );
    }
}
