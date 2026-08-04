<?php

declare(strict_types=1);

use Ramsey\Uuid\Uuid;
use Spoolrail\Spoolrail\Message;

test('creates an unpublished UUIDv7 message with a caller-defined payload', function (): void {
    $type = 'order.created';
    $payload = [
        'order_id' => 42,
    ];

    $message = Message::make($type, $payload);

    expect(Uuid::fromString($message->id)->getFields()->getVersion())
        ->toBe(Uuid::UUID_TYPE_UNIX_TIME);
    expect($message->type)->toBe($type);
    expect($message->payload)->toBe($payload);
    expect($message->publishedAt)->toBeNull();
    expect($message->transport)->toBeNull();
});

test('rejects empty and whitespace-only message types', function (string $type): void {
    expect(fn (): Message => Message::make($type, []))
        ->toThrow(InvalidArgumentException::class);
})->with([
    'empty' => '',
    'whitespace' => " \t\n",
]);

test('accepts a message type at the portable byte limit', function (): void {
    $message = Message::make(str_repeat('a', 255), []);

    expect(strlen($message->type))->toBe(255);
});

test('rejects message types that cannot cross the portable boundary', function (string $type): void {
    expect(fn (): Message => Message::make($type, []))
        ->toThrow(
            InvalidArgumentException::class,
            'The message type must be a non-empty valid UTF-8 string of at most 255 bytes.',
        );
})->with([
    'invalid UTF-8' => "\xFF",
    'over 255 UTF-8 bytes' => str_repeat('é', 128),
]);
