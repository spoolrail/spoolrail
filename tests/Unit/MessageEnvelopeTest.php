<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageEnvelopeException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;

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

test('rejects message IDs that are not UUIDv7', function (string $id): void {
    $envelope = new MessageEnvelope;
    $json = createMessageEnvelopeJson(['id' => $id]);

    expect(fn (): Message => $envelope->decode($json))
        ->toThrow(
            InvalidMessageEnvelopeException::class,
            'The message envelope must contain a valid UUIDv7 ID.',
        );
})->with([
    'malformed UUID' => 'not-a-uuid',
    'different UUID version' => 'f81d4fae-7dec-4a0d-a765-00a0c91e6bf6',
]);

test('rejects invalid message types', function (mixed $type): void {
    $envelope = new MessageEnvelope;
    $json = createMessageEnvelopeJson(['type' => $type]);

    expect(fn (): Message => $envelope->decode($json))
        ->toThrow(
            InvalidMessageEnvelopeException::class,
            'The message envelope must contain a non-empty type.',
        );
})->with([
    'whitespace string' => " \t\n",
    'number' => 42,
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

test('rejects non-canonical publication timestamps', function (string $publishedAt): void {
    $envelope = new MessageEnvelope;
    $json = createMessageEnvelopeJson(['published_at' => $publishedAt]);

    expect(fn (): Message => $envelope->decode($json))
        ->toThrow(
            InvalidMessageEnvelopeException::class,
            'The message envelope must contain a valid canonical UTC millisecond timestamp.',
        );
})->with([
    'offset instead of UTC' => '2026-07-15T17:23:08.417+03:00',
    'missing millisecond precision' => '2026-07-15T14:23:08Z',
    'microsecond precision' => '2026-07-15T14:23:08.417000Z',
    'invalid date' => '2026-99-15T14:23:08.417Z',
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
