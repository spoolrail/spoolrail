<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithRabbitMq;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

uses(InteractsWithRabbitMq::class);

test('publishes through the next available RabbitMQ host to every subscription', function (): void {
    // --- Arrange ---
    $connection = config('spoolrail.connections.rabbitmq');
    $reachableHost = $connection['host'];
    unset($connection['host']);
    $connection['hosts'] = ['unavailable.invalid', $reachableHost];
    $connection['connection_timeout'] = 1;
    config()->set('spoolrail.connections.rabbitmq', $connection);

    Spoolrail::subscribe('orders', 'warehouse', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');
    Spoolrail::subscribe('orders', 'analytics', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');
    $this->artisan('spoolrail:ensure-topology')->run();
    $headers = [
        str_repeat('a', 128) => str_repeat('é', 512),
        'header-2' => 'value-2',
        'header-3' => 'value-3',
        'header-4' => 'value-4',
        'header-5' => 'value-5',
        'header-6' => 'value-6',
        'header-7' => 'value-7',
        'header-8' => 'value-8',
        'header-9' => 'value-9',
        'header-10' => 'value-10',
    ];

    // --- Act ---
    $published = Spoolrail::connection('rabbitmq')->publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
        headers: $headers,
    );

    // --- Assert ---
    $envelope = new MessageEnvelope;
    $deliveries = [];
    $properties = [];
    $prefix = app(OwnershipPrefix::class)->current();

    foreach (["$prefix-warehouse", "$prefix-analytics"] as $queue) {
        $delivery = $this->drainRabbitMqDeliveries($queue, 1)[0];
        $deliveries[] = $envelope->decode($delivery['payload']);
        $properties[] = $delivery['properties'];
    }

    expect($deliveries)->toEqual([$published, $published]);
    expect($properties)->each->toMatchArray([
        'message_id' => $published->id,
        'type' => $published->type,
        'timestamp' => $published->publishedAt?->getTimestamp(),
        'headers' => $headers,
    ]);
});
