<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithRabbitMq;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

uses(InteractsWithRabbitMq::class);

test('returns every unsettled prefetched RabbitMQ delivery after a failed handoff', function (): void {
    // --- Arrange ---
    $queue = app(OwnershipPrefix::class)->current().'-warehouse';

    config()->set('spoolrail.connections.rabbitmq.prefetch', 3);

    Spoolrail::subscribe('orders', 'warehouse', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');
    $this->artisan('spoolrail:sync')->run();

    foreach (['first', 'second', 'third', 'fourth'] as $reference) {
        Spoolrail::connection('rabbitmq')->publish(
            'orders',
            Message::make('order.created', ['reference' => $reference]),
        );
    }

    $envelope = new MessageEnvelope;
    $handoffs = [];
    $failure = new RuntimeException('Laravel Queue handoff failed.');
    $caught = null;

    // --- Act ---
    try {
        Spoolrail::connection('rabbitmq')->consume(
            'warehouse',
            function (string $body) use ($envelope, &$handoffs, $failure): void {
                $reference = $envelope->decode($body)->payload['reference'];
                $handoffs[] = $reference;

                if ($reference === 'second') {
                    throw $failure;
                }
            },
        );
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    $remaining = array_map(
        fn (array $delivery): array => [
            'reference' => $envelope->decode($delivery['payload'])->payload['reference'],
            'redelivered' => $delivery['redelivered'],
        ],
        $this->drainRabbitMqDeliveries($queue, 4),
    );
    usort(
        $remaining,
        static fn (array $left, array $right): int => $left['reference'] <=> $right['reference'],
    );

    expect($caught)->toBe($failure);
    expect($handoffs)->toBe(['first', 'second']);
    expect($remaining)->toBe([
        ['reference' => 'fourth', 'redelivered' => true],
        ['reference' => 'second', 'redelivered' => true],
        ['reference' => 'third', 'redelivered' => true],
    ]);
});
