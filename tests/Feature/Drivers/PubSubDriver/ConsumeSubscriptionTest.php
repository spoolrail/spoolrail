<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithPubSub;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\TransportContext;

uses(InteractsWithPubSub::class);

test('passes Pub/Sub deliveries to Queue until the handoff fails', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');
    $this->artisan('spoolrail:sync')->run();

    $first = Spoolrail::connection('pubsub')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'first']),
    );
    $second = Spoolrail::connection('pubsub')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'second']),
    );
    $handoffs = [];
    $failure = new RuntimeException('Stop on the second handoff.');

    // --- Act ---
    try {
        Spoolrail::connection('pubsub')->consume(
            'warehouse-orders',
            function (string $body, TransportContext $_context) use (&$handoffs, $failure): void {
                $handoffs[] = $body;

                if (count($handoffs) === 2) {
                    throw $failure;
                }
            },
        );
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught ?? null)->toBe($failure);
    expect($handoffs)->toHaveCount(2);
    $receivedIds = array_map(
        static fn (string $body): string => json_decode(
            $body,
            true,
            flags: JSON_THROW_ON_ERROR,
        )['id'],
        $handoffs,
    );
    expect($receivedIds)->toContain($first->id);
    expect($receivedIds)->toContain($second->id);
});
