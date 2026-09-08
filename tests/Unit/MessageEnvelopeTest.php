<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageEnvelopeException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\TransportContext;

test('encodes the exact wire envelope', function (): void {
    // --- Arrange ---
    $envelope = new MessageEnvelope;
    $message = Message::make('order.created', [
        'items' => [
            ['sku' => 'ABC', 'quantity' => 2],
        ],
        'metadata' => ['gift_message' => null],
        'paid' => false,
    ])->withPublishedAt(CarbonImmutable::parse('2026-07-15 14:23:08.417000 UTC'));
    $message = $message->withTransport(new TransportContext(
        driver: 'array',
        connectionName: 'array',
        topic: 'orders',
        subscription: 'warehouse-orders',
        headers: ['correlation-id' => 'A-42'],
        redelivered: false,
    ));
    $expected = json_encode([
        'id' => $message->id,
        'type' => 'order.created',
        'payload' => [
            'items' => [
                ['sku' => 'ABC', 'quantity' => 2],
            ],
            'metadata' => ['gift_message' => null],
            'paid' => false,
        ],
        'published_at' => '2026-07-15T14:23:08.417Z',
    ], JSON_THROW_ON_ERROR);

    // --- Act ---
    $json = $envelope->encode($message);

    // --- Assert ---
    expect($json)->toBe($expected);
});

test('decodes only supported fields from a wire envelope', function (): void {
    // --- Arrange ---
    $id = '01890a5d-ac96-774b-bcd0-48f622f3e798';
    $json = json_encode([
        'id' => $id,
        'type' => 'order.created',
        'payload' => [
            'items' => [
                ['sku' => 'ABC', 'quantity' => 2],
            ],
            'paid' => true,
        ],
        'published_at' => '2026-07-15T14:23:08.417Z',
        'future_field' => ['ignored' => true],
    ], JSON_THROW_ON_ERROR);

    // --- Act ---
    $message = (new MessageEnvelope)->decode($json);

    // --- Assert ---
    expect($message->id)->toBe($id);
    expect($message->type)->toBe('order.created');
    expect($message->payload)->toBe([
        'items' => [
            ['sku' => 'ABC', 'quantity' => 2],
        ],
        'paid' => true,
    ]);
    expect($message->publishedAt?->timezoneName)->toBe('UTC');
    expect($message->publishedAt?->format('Y-m-d\TH:i:s.v\Z'))->toBe('2026-07-15T14:23:08.417Z');
    expect($message->transport)->toBeNull();
});

test('rejects JSON values that are not object envelopes', function (string $json): void {
    $envelope = new MessageEnvelope;

    expect(fn (): Message => $envelope->decode($json))
        ->toThrow(InvalidMessageEnvelopeException::class, 'The message envelope must be a JSON object.');
})->with([
    'array' => '[]',
    'scalar' => 'null',
]);

test('rejects malformed JSON', function (): void {
    expect(fn (): Message => (new MessageEnvelope)->decode(''))
        ->toThrow(function (InvalidMessageEnvelopeException $exception): void {
            expect($exception->getMessage())->toBe('The message envelope must contain valid JSON.');
            expect($exception->getPrevious())->toBeInstanceOf(JsonException::class);
        });
});

test('rejects wire envelopes missing a required field', function (string $field): void {
    $envelope = new MessageEnvelope;
    $json = createMessageEnvelopeJson(missing: $field);

    expect(fn (): Message => $envelope->decode($json))
        ->toThrow(InvalidMessageEnvelopeException::class);
})->with([
    'ID' => 'id',
    'type' => 'type',
    'payload' => 'payload',
    'publication timestamp' => 'published_at',
]);

test('rejects malformed message IDs', function (): void {
    $envelope = new MessageEnvelope;
    $json = createMessageEnvelopeJson(['id' => 'not-a-uuid']);

    expect(fn (): Message => $envelope->decode($json))
        ->toThrow(
            InvalidMessageEnvelopeException::class,
            'The message envelope must contain a valid UUID.',
        );
});

test('rejects invalid message types', function (mixed $type): void {
    $envelope = new MessageEnvelope;
    $json = createMessageEnvelopeJson(['type' => $type]);

    expect(fn (): Message => $envelope->decode($json))
        ->toThrow(
            InvalidMessageEnvelopeException::class,
            'The message envelope must contain a non-empty valid UTF-8 type of at most 255 bytes.',
        );
})->with([
    'whitespace string' => " \t\n",
    'number' => 42,
    'over 255 UTF-8 bytes' => str_repeat('é', 128),
]);

test('rejects payloads that are not arrays', function (): void {
    $envelope = new MessageEnvelope;
    $json = createMessageEnvelopeJson(['payload' => 'not-an-array']);

    expect(fn (): Message => $envelope->decode($json))
        ->toThrow(
            InvalidMessageEnvelopeException::class,
            'The message envelope must contain an array payload.',
        );
});

test('normalizes explicitly zoned publication timestamps without losing precision', function (string $timestamp, string $utc): void {
    // --- Arrange ---
    $json = createMessageEnvelopeJson(['published_at' => $timestamp]);

    // --- Act ---
    $message = (new MessageEnvelope)->decode($json);

    // --- Assert ---
    expect($message->publishedAt?->format('Y-m-d\TH:i:s.uP'))->toBe($utc);
})->with([
    'whole seconds' => ['2026-07-15T14:23:08Z', '2026-07-15T14:23:08.000000+00:00'],
    'short fraction' => ['2026-07-15T14:23:08.4Z', '2026-07-15T14:23:08.400000+00:00'],
    'microseconds' => ['2026-07-15T14:23:08.417123Z', '2026-07-15T14:23:08.417123+00:00'],
    'negative offset' => ['2026-07-15T09:23:08-05:00', '2026-07-15T14:23:08.000000+00:00'],
    'fractional-hour offset' => ['2026-07-15T19:53:08.417+05:30', '2026-07-15T14:23:08.417000+00:00'],
]);

test('rejects invalid publication timestamps', function (string $timestamp): void {
    $json = createMessageEnvelopeJson(['published_at' => $timestamp]);

    expect(fn (): Message => (new MessageEnvelope)->decode($json))
        ->toThrow(InvalidMessageEnvelopeException::class);
})->with([
    'missing timezone' => '2026-07-15T14:23:08',
    'impossible date' => '2026-02-30T14:23:08Z',
    'overflowing offset minutes' => '2026-07-15T14:23:08+03:60',
    'overflowing offset hours' => '2026-07-15T14:23:08+24:00',
    'null byte' => "2026-07-15T14:23:08\0Z",
    'excess precision' => '2026-07-15T14:23:08.4171234Z',
]);

/**
 * @param  array<string, mixed>  $overrides
 *
 * @throws JsonException
 */
function createMessageEnvelopeJson(array $overrides = [], ?string $missing = null): string
{
    $envelope = array_replace([
        'id' => '01890a5d-ac96-774b-bcd0-48f622f3e798',
        'type' => 'order.created',
        'payload' => ['reference' => 'A-42'],
        'published_at' => '2026-07-15T14:23:08.417Z',
    ], $overrides);

    if ($missing !== null) {
        unset($envelope[$missing]);
    }

    return json_encode($envelope, JSON_THROW_ON_ERROR);
}
