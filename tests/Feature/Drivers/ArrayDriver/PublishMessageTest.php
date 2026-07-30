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
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'analytics-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('returns', 'warehouse-returns', RecordingMessageHandler::class);

    // --- Act ---
    $published = Spoolrail::publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
    );
    $this->artisan('spoolrail:consume warehouse-orders')->run();
    $this->artisan('spoolrail:consume analytics-orders')->run();
    $this->artisan('spoolrail:consume warehouse-returns')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$messages)->toEqual([$published, $published]);
    expect(RecordingMessageHandler::$messages[0])->not->toBe($published);
    expect(RecordingMessageHandler::$messages[1])->not->toBe($published);
    expect(RecordingMessageHandler::$messages[1])->not->toBe(RecordingMessageHandler::$messages[0]);
});

test('publishes only to subscriptions on the selected Spoolrail connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.secondary', ['driver' => 'array']);

    Spoolrail::subscribe('orders', 'default-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'secondary-orders', RecordingMessageHandler::class)
        ->onConnection('secondary');

    // --- Act ---
    Spoolrail::connection('secondary')->publish(
        'orders',
        Message::make('activity.recorded', ['reference' => 'secondary']),
    );
    $this->artisan('spoolrail:consume default-orders')->run();
    $this->artisan('spoolrail:consume secondary-orders')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$messages)->toHaveCount(1);
    expect(RecordingMessageHandler::$messages[0]->payload['reference'])->toBe('secondary');
});
