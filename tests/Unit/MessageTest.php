<?php

declare(strict_types=1);

use Ramsey\Uuid\Uuid;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageException;
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
});

test('rejects empty and whitespace-only message types', function (string $type): void {
    expect(fn (): Message => Message::make($type, []))
        ->toThrow(InvalidMessageException::class);
})->with([
    'empty' => '',
    'whitespace' => " \t\n",
]);
