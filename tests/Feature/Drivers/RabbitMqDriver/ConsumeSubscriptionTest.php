<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithRabbitMq;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Spoolrail\Spoolrail\TransportContext;

uses(InteractsWithRabbitMq::class);

test('returns every unsettled prefetched RabbitMQ delivery after a failed handoff', function (): void {
    // --- Arrange ---
    $queue = app(OwnershipPrefix::class)->current().'-warehouse';

    config()->set('spoolrail.connections.rabbitmq.prefetch', 3);

    Spoolrail::subscribe('orders', 'warehouse', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');
    $this->artisan('spoolrail:sync')->run();
    $longHeader = str_repeat('a', 128);

    foreach (['first', 'second', 'third', 'fourth'] as $reference) {
        Spoolrail::connection('rabbitmq')->publish(
            'orders',
            Message::make('order.created', ['reference' => $reference]),
            headers: [
                $reference === 'first' ? $longHeader : 'correlation-id' => $reference,
            ],
        );
    }

    $envelope = new MessageEnvelope;
    $handoffs = [];
    $contexts = [];
    $failure = new RuntimeException('Laravel Queue handoff failed.');
    $caught = null;

    // --- Act ---
    try {
        Spoolrail::connection('rabbitmq')->consume(
            'warehouse',
            function (string $body, TransportContext $transport) use ($envelope, &$handoffs, &$contexts, $failure): void {
                $reference = $envelope->decode($body)->payload['reference'];
                $handoffs[] = $reference;
                $contexts[] = $transport;

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
    expect($contexts[0]->driver)->toBe('rabbitmq');
    expect($contexts[0]->connectionName)->toBe('rabbitmq');
    expect($contexts[0]->topic)->toBe('orders');
    expect($contexts[0]->subscription)->toBe('warehouse');
    expect($contexts[0]->headers)->toBe([$longHeader => 'first']);
    expect($contexts[0]->transportMessageId)->toBeNull();
    expect($contexts[0]->transportPublishedAt)->toBeNull();
    expect($contexts[0]->redelivered)->toBeFalse();
    expect($contexts[1]->headers)->toBe(['correlation-id' => 'second']);
    expect($remaining)->toBe([
        ['reference' => 'fourth', 'redelivered' => true],
        ['reference' => 'second', 'redelivered' => true],
        ['reference' => 'third', 'redelivered' => true],
    ]);
});
