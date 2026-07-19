<?php

declare(strict_types=1);

use Ramsey\Uuid\Uuid;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageException;
use Spoolrail\Spoolrail\Message;

test('creates an unpublished UUIDv7 message with caller-defined data', function (): void {
    // --- Arrange ---
    $type = 'order.created';
    $payload = [
        'order_id' => 42,
    ];

    // --- Act ---
    $message = Message::make($type, $payload);

    // --- Assert ---
    expect(Uuid::fromString($message->id)->getFields()->getVersion())
        ->toBe(Uuid::UUID_TYPE_UNIX_TIME);
    expect($message->type)->toBe($type);
    expect($message->payload)->toBe($payload);
    expect($message->publishedAt)->toBeNull();
});

test('prevents callers from changing the message envelope', function (): void {
    // --- Arrange ---
    $message = Message::make('order.created', ['order_id' => 42]);

    // --- Act ---
    $typeFailure = null;

    try {
        $message->type = 'order.cancelled';
    } catch (Throwable $exception) {
        $typeFailure = $exception;
    }

    $payloadFailure = null;

    try {
        $message->payload['order_id'] = 84;
    } catch (Throwable $exception) {
        $payloadFailure = $exception;
    }

    // --- Assert ---
    expect($typeFailure)->toBeInstanceOf(Error::class);
    expect($payloadFailure)->toBeInstanceOf(Error::class);
});

test('rejects empty and whitespace-only message types', function (string $type): void {
    // --- Arrange ---
    $payload = [];

    // --- Act ---
    $failure = null;

    try {
        Message::make($type, $payload);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidMessageException::class);
})->with([
    'empty' => '',
    'whitespace' => " \t\n",
]);
