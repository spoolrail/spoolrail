<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

beforeEach(function (): void {
    RecordingMessageHandler::reset();
});

test('publishes one independently hydrated delivery to every matching subscription', function (): void {
    // --- Arrange ---
    config()->set('queue.default', 'sync');

    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'analytics-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('returns', 'warehouse-returns', RecordingMessageHandler::class);

    // --- Act ---
    $published = Spoolrail::publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
        headers: ['correlation-id' => 'A-42'],
    );
    $this->artisan('spoolrail warehouse-orders')->run();
    $this->artisan('spoolrail analytics-orders')->run();
    $this->artisan('spoolrail warehouse-returns')->run();

    // --- Assert ---
    expect($published->transport)->toBeNull();
    expect(array_map(
        static fn (Message $message): string => $message->id,
        RecordingMessageHandler::$messages,
    ))->toBe([$published->id, $published->id]);
    expect(array_map(
        static fn (Message $message): array => $message->payload,
        RecordingMessageHandler::$messages,
    ))->toBe([$published->payload, $published->payload]);
    expect(RecordingMessageHandler::$messages[0])->not->toBe($published);
    expect(RecordingMessageHandler::$messages[1])->not->toBe($published);
    expect(RecordingMessageHandler::$messages[1])->not->toBe(RecordingMessageHandler::$messages[0]);
    expect(RecordingMessageHandler::$messages[0]->transport?->driver)->toBe('array');
    expect(RecordingMessageHandler::$messages[0]->transport?->connectionName)->toBe('array');
    expect(RecordingMessageHandler::$messages[0]->transport?->topic)->toBe('orders');
    expect(RecordingMessageHandler::$messages[0]->transport?->subscription)->toBe('warehouse-orders');
    expect(RecordingMessageHandler::$messages[0]->transport?->headers)
        ->toBe(['correlation-id' => 'A-42']);
    expect(RecordingMessageHandler::$messages[0]->transport?->transportMessageId)->toBeNull();
    expect(RecordingMessageHandler::$messages[0]->transport?->transportPublishedAt)->toBeNull();
    expect(RecordingMessageHandler::$messages[0]->transport?->redelivered)->toBeFalse();
    expect(RecordingMessageHandler::$messages[1]->transport?->subscription)->toBe('analytics-orders');
    expect(RecordingMessageHandler::$messages[1]->transport?->headers)
        ->toBe(['correlation-id' => 'A-42']);
});

test('publishes only to subscriptions on the selected Spoolrail connection', function (): void {
    // --- Arrange ---
    config()->set('queue.default', 'sync');
    config()->set('spoolrail.connections.secondary', ['driver' => 'array']);

    Spoolrail::subscribe('orders', 'default-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'secondary-orders', RecordingMessageHandler::class)
        ->onConnection('secondary');

    // --- Act ---
    Spoolrail::connection('secondary')->publish(
        'orders',
        Message::make('activity.recorded', ['reference' => 'secondary']),
    );
    $this->artisan('spoolrail default-orders')->run();
    $this->artisan('spoolrail secondary-orders')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$messages)->toHaveCount(1);
    expect(RecordingMessageHandler::$messages[0]->payload['reference'])->toBe('secondary');
    expect(RecordingMessageHandler::$messages[0]->transport?->connectionName)->toBe('secondary');
});

test('republishes only explicitly supplied headers without changing the received message', function (): void {
    // --- Arrange ---
    config()->set('queue.default', 'sync');

    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('returns', 'warehouse-returns', RecordingMessageHandler::class);

    Spoolrail::publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
        headers: ['correlation-id' => 'inbound'],
    );
    $this->artisan('spoolrail warehouse-orders')->run();

    $received = RecordingMessageHandler::$messages[0];
    $inboundTransport = $received->transport;

    // --- Act ---
    $republished = Spoolrail::publish('returns', $received);
    $this->artisan('spoolrail warehouse-returns')->run();

    // --- Assert ---
    $deliveredAgain = RecordingMessageHandler::$messages[1];

    expect($received->transport)->toBe($inboundTransport);
    expect($received->transport?->headers)->toBe(['correlation-id' => 'inbound']);
    expect($republished->id)->toBe($received->id);
    expect($republished->transport)->toBeNull();
    expect($deliveredAgain->id)->toBe($received->id);
    expect($deliveredAgain->transport?->topic)->toBe('returns');
    expect($deliveredAgain->transport?->subscription)->toBe('warehouse-returns');
    expect($deliveredAgain->transport?->headers)->toBe([]);
});
