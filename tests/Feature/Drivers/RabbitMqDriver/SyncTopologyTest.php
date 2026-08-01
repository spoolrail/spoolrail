<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithRabbitMq;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

uses(InteractsWithRabbitMq::class);

test('synchronizes every referenced RabbitMQ connection after preflight', function (): void {
    // --- Arrange ---
    $this->addRabbitMqConnection('secondary');
    $prefix = app(OwnershipPrefix::class)->current();

    Spoolrail::subscribe('orders', 'rabbit-orders', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');
    Spoolrail::subscribe('orders', 'secondary-orders', RecordingMessageHandler::class)
        ->onConnection('secondary');
    Spoolrail::subscribe('orders', 'local-orders', RecordingMessageHandler::class);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:sync')
        ->expectsOutputToContain('Synchronized topology for connection [rabbitmq].')
        ->expectsOutputToContain('Synchronized topology for connection [secondary].')
        ->expectsOutputToContain('Connection [array] has no package-managed topology and was not changed.')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->rabbitMqQueueExists("$prefix-rabbit-orders"))->toBeTrue();
    expect($this->rabbitMqQueueExists("$prefix-secondary-orders"))->toBeTrue();
});

test('reports every RabbitMQ preflight failure before applying a valid plan', function (): void {
    // --- Arrange ---
    $this->addRabbitMqConnection('secondary');
    $this->addRabbitMqConnection('tertiary');
    $prefix = app(OwnershipPrefix::class)->current();

    $this->declareRabbitMqExchange('secondary-orders', type: 'direct');
    $this->declareRabbitMqExchange('tertiary-orders', type: 'direct');

    Spoolrail::subscribe('orders', 'rabbit-orders', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');
    Spoolrail::subscribe('secondary-orders', 'secondary-orders', RecordingMessageHandler::class)
        ->onConnection('secondary');
    Spoolrail::subscribe('tertiary-orders', 'tertiary-orders', RecordingMessageHandler::class)
        ->onConnection('tertiary');

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:sync')->run();
    } catch (TopologyPreflightException $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(TopologyPreflightException::class);
    expect(array_keys($failure?->failuresByConnection ?? []))->toBe(['secondary', 'tertiary']);
    expect($failure?->failuresByConnection['secondary'] ?? null)->toBeInstanceOf(RabbitMqTopologyException::class);
    expect($failure?->failuresByConnection['tertiary'] ?? null)->toBeInstanceOf(RabbitMqTopologyException::class);
    expect($this->rabbitMqQueueExists("$prefix-rabbit-orders"))->toBeFalse();
});

test('accepts repeated synchronization without replacing an existing quorum queue', function (): void {
    // --- Arrange ---
    $this->defaultToQuorumQueues();

    $queueName = app(OwnershipPrefix::class)->current().'-warehouse';

    Spoolrail::subscribe('orders', 'warehouse', RecordingMessageHandler::class)
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
    expect((new MessageEnvelope)->decode($body))->toEqual($published);
});
