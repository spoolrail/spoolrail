<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageSerializer;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithRabbitMq;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

uses(InteractsWithRabbitMq::class);

test('accepts repeated synchronization without replacing an existing quorum queue', function (): void {
    // --- Arrange ---
    $this->useQuorumQueues();

    $queueName = app(OwnershipPrefix::class)->value().'-warehouse';

    Spoolrail::subscribe('orders', 'warehouse', NoopMessageHandler::class)
        ->onConnection('rabbitmq');

    // --- Act ---
    $firstSync = $this->artisan('spoolrail:sync')->run();
    $published = Spoolrail::connection('rabbitmq')->publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
    );
    $secondSync = $this->artisan('spoolrail:sync')->run();

    // --- Assert ---
    $queue = $this->rabbitMqQueue($queueName);
    $body = $this->drainRabbitMqDeliveries($queueName, 1)[0]['payload'];

    expect($firstSync)->toBe(0);
    expect($secondSync)->toBe(0);
    expect($queue)->toMatchArray(['type' => 'quorum']);
    expect(data_get($queue, 'arguments.x-delivery-limit'))->toBe(-1);
    expect((new MessageSerializer)->deserialize($body))->toEqual($published);
});
