<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;
use DateTimeImmutable;
use DateTimeZone;
use JsonException;
use Ramsey\Uuid\Rfc4122\FieldsInterface;
use Ramsey\Uuid\Uuid;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageEnvelopeException;

/**
 * @internal
 */
class MessageEnvelope
{
    private const string TIMESTAMP_FORMAT = 'Y-m-d\TH:i:s.v\Z';

    public function encode(Message $message): string
    {
        return json_encode([
            'id' => $message->id,
            'type' => $message->type,
            'payload' => $message->payload,
            'published_at' => $message->publishedAt?->format(self::TIMESTAMP_FORMAT),
        ], JSON_THROW_ON_ERROR);
    }

    public function decode(string $json): Message
    {
        try {
            $envelope = json_decode($json, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw InvalidMessageEnvelopeException::malformedJson($exception);
        }

        if (! is_array($envelope) || ! str_starts_with(ltrim($json), '{')) {
            throw InvalidMessageEnvelopeException::mustBeObject();
        }

        return Message::fromEnvelope(
            $this->id($envelope),
            $this->type($envelope),
            $this->payload($envelope),
            $this->publishedAt($envelope),
        );
    }

    /**
     * @param  array<array-key, mixed>  $envelope
     */
    private function id(array $envelope): string
    {
        $id = $envelope['id'] ?? null;

        if (! is_string($id) || ! Uuid::isValid($id)) {
            throw InvalidMessageEnvelopeException::invalidId();
        }

        $fields = Uuid::fromString($id)->getFields();

        if ($fields instanceof FieldsInterface && $fields->getVersion() === Uuid::UUID_TYPE_UNIX_TIME) {
            return $id;
        }

        throw InvalidMessageEnvelopeException::invalidId();
    }

    /**
     * @param  array<array-key, mixed>  $envelope
     */
    private function type(array $envelope): string
    {
        $type = $envelope['type'] ?? null;

        if (is_string($type) && Message::isValidType($type)) {
            return $type;
        }

        throw InvalidMessageEnvelopeException::invalidType();
    }

    /**
     * @param  array<array-key, mixed>  $envelope
     * @return array<array-key, mixed>
     */
    private function payload(array $envelope): array
    {
        $payload = $envelope['payload'] ?? null;

        if (is_array($payload)) {
            return $payload;
        }

        throw InvalidMessageEnvelopeException::payloadMustBeArray();
    }

    /**
     * @param  array<array-key, mixed>  $envelope
     */
    private function publishedAt(array $envelope): CarbonImmutable
    {
        $timestamp = $envelope['published_at'] ?? null;

        if (! is_string($timestamp)) {
            throw InvalidMessageEnvelopeException::invalidTimestamp();
        }

        $publishedAt = DateTimeImmutable::createFromFormat(
            '!'.self::TIMESTAMP_FORMAT,
            $timestamp,
            new DateTimeZone('UTC'),
        );

        if ($publishedAt === false || $publishedAt->format(self::TIMESTAMP_FORMAT) !== $timestamp) {
            throw InvalidMessageEnvelopeException::invalidTimestamp();
        }

        return CarbonImmutable::instance($publishedAt);
    }
}
