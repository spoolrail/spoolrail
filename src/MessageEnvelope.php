<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Carbon\CarbonImmutable;
use DateTimeImmutable;
use DateTimeZone;
use JsonException;
use Ramsey\Uuid\Uuid;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageEnvelopeException;

class MessageEnvelope
{
    private const string TIMESTAMP_FORMAT = 'Y-m-d\TH:i:s.v\Z';

    public function encode(Message $message): string
    {
        return json_encode($this->toArray($message), JSON_THROW_ON_ERROR);
    }

    /**
     * @return array{
     *     id: string,
     *     type: string,
     *     payload: array<array-key, mixed>,
     *     published_at: string|null
     * }
     */
    public function toArray(Message $message): array
    {
        return [
            'id' => $message->id,
            'type' => $message->type,
            'payload' => $message->payload,
            'published_at' => $message->publishedAt?->format(self::TIMESTAMP_FORMAT),
        ];
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

        return $this->fromArray($envelope);
    }

    /**
     * @param  array<array-key, mixed>  $envelope
     */
    public function fromArray(array $envelope): Message
    {
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

        /*
         * Publish UUIDv7 IDs, but accept any UUID version on receipt so a future
         * version change need not require consumers to be deployed first.
         */
        if (! is_string($id) || ! Uuid::isValid($id)) {
            throw InvalidMessageEnvelopeException::invalidId();
        }

        return $id;
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

        if (! is_string($timestamp) || str_contains($timestamp, "\0")) {
            throw InvalidMessageEnvelopeException::invalidTimestamp();
        }

        /*
         * Publish one canonical format, but accept equivalent representations on receipt
         * so future timestamp format changes need not require consumers to be deployed first.
         */
        $formats = [
            'Y-m-d\TH:i:s\Z', // 2026-07-15T14:23:08Z
            self::TIMESTAMP_FORMAT, // 2026-07-15T14:23:08.417Z
            'Y-m-d\TH:i:s.u\Z', // 2026-07-15T14:23:08.417123Z
            'Y-m-d\TH:i:sP', // 2026-07-15T17:23:08+03:00
            'Y-m-d\TH:i:s.vP', // 2026-07-15T17:23:08.417+03:00
            'Y-m-d\TH:i:s.uP', // 2026-07-15T17:23:08.417123+03:00
        ];

        foreach ($formats as $format) {
            $publishedAt = DateTimeImmutable::createFromFormat('!'.$format, $timestamp, new DateTimeZone('UTC'));
            if ($publishedAt === false) {
                continue;
            }
            if (DateTimeImmutable::getLastErrors() !== false) {
                continue;
            }

            if (abs($publishedAt->getOffset()) >= 24 * 60 * 60) {
                continue;
            }

            // Carbon's P validator restricts offset minutes, so match the parsed offset literally.
            $validationFormat = str_replace('P', $publishedAt->format('P'), $format);

            if (CarbonImmutable::hasFormat($timestamp, $validationFormat)) {
                return CarbonImmutable::instance($publishedAt)->utc();
            }
        }

        throw InvalidMessageEnvelopeException::invalidTimestamp();
    }
}
