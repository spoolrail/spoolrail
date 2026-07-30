<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageSerializer;
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
    $this->artisan('spoolrail:sync')->run();

    // --- Act ---
    $published = Spoolrail::connection('rabbitmq')->publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
    );

    // --- Assert ---
    $serializer = new MessageSerializer;
    $deliveries = [];
    $prefix = app(OwnershipPrefix::class)->current();

    foreach (["$prefix-warehouse", "$prefix-analytics"] as $queue) {
        $body = $this->drainRabbitMqDeliveries($queue, 1)[0]['payload'];
        $deliveries[] = $serializer->deserialize($body);
    }

    expect($deliveries)->toEqual([$published, $published]);
});
