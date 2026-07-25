<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageSerializer;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;
use Spoolrail\Spoolrail\Tests\Fixtures\RabbitMqTestVhost;

test('publishes through the next available RabbitMQ host to every subscription', function (): void {
    $rabbitMq = RabbitMqTestVhost::create();

    try {
        // --- Arrange ---
        $connection = config('spoolrail.connections.rabbitmq');
        $reachableHost = $connection['host'];
        unset($connection['host']);
        $connection['hosts'] = ['unavailable.invalid', $reachableHost];
        $connection['connection_timeout'] = 1;
        config()->set('spoolrail.connections.rabbitmq', $connection);

        Spoolrail::subscribe('orders', 'warehouse', NoopMessageHandler::class)
            ->onConnection('rabbitmq');
        Spoolrail::subscribe('orders', 'analytics', NoopMessageHandler::class)
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
        $prefix = app(OwnershipPrefix::class)->value();

        foreach (["$prefix-warehouse", "$prefix-analytics"] as $queue) {
            $body = $rabbitMq->management
                ->post(
                    '/api/queues/'.rawurlencode($rabbitMq->name).'/'.rawurlencode($queue).'/get',
                    [
                        'count' => 1,
                        'ackmode' => 'ack_requeue_false',
                        'encoding' => 'auto',
                        'truncate' => 300_000,
                    ],
                )
                ->throw()
                ->json('0.payload');

            $deliveries[] = $serializer->deserialize($body);
        }

        expect($deliveries)->toEqual([$published, $published]);
    } finally {
        $rabbitMq->delete();
    }
});
