<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;
use Spoolrail\Spoolrail\Tests\Fixtures\RabbitMqTestBroker;

test('creates the declared durable fanout topology idempotently', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create();
    $broker->configureApplication();
    $topic = 'orders_'.bin2hex(random_bytes(4));
    $subscription = 'warehouse_'.bin2hex(random_bytes(4));
    $queueName = $broker->queueName($subscription);

    Spoolrail::subscribe($topic, $subscription, NoopMessageHandler::class);

    try {
        // --- Act ---
        $firstExit = $this->artisan('spoolrail:sync')->run();
        $firstExchange = $broker->exchange($topic);
        $firstQueue = $broker->queue($queueName);
        $firstBindings = array_values(array_filter(
            $broker->queueBindings($queueName),
            static fn (array $binding): bool => ($binding['source'] ?? '') !== '',
        ));

        $secondExit = $this->artisan('spoolrail:sync')->run();

        // --- Assert ---
        expect($firstExit)->toBe(0);
        expect($secondExit)->toBe(0);
        expect($firstExchange)->toMatchArray([
            'name' => $topic,
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
            'arguments' => [],
        ]);
        expect($firstQueue)->toMatchArray([
            'name' => $queueName,
            'type' => 'classic',
            'durable' => true,
            'exclusive' => false,
            'auto_delete' => false,
        ]);
        expect(array_diff_key(
            $firstQueue['arguments'] ?? [],
            ['x-queue-type' => true],
        ))->toBe([]);
        expect($firstBindings)->toHaveCount(1);
        expect($firstBindings[0])->toMatchArray([
            'source' => $topic,
            'destination' => $queueName,
            'destination_type' => 'queue',
            'routing_key' => '',
            'arguments' => [],
        ]);
        $secondBindings = array_values(array_filter(
            $broker->queueBindings($queueName),
            static fn (array $binding): bool => ($binding['source'] ?? '') !== '',
        ));

        expect($broker->exchange($topic))->not->toBeNull();
        expect($broker->queue($queueName))->not->toBeNull();
        expect($secondBindings)->toHaveCount(1);
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});

test('declares an unlimited delivery limit when the broker default is quorum', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create('quorum');
    $broker->configureApplication();
    $topic = 'orders_'.bin2hex(random_bytes(4));
    $subscription = 'warehouse_'.bin2hex(random_bytes(4));
    $queueName = $broker->queueName($subscription);

    Spoolrail::subscribe($topic, $subscription, NoopMessageHandler::class);

    try {
        // --- Act ---
        $exit = $this->artisan('spoolrail:sync')->run();

        // --- Assert ---
        expect($exit)->toBe(0);
        $queue = $broker->queue($queueName);

        expect($queue)->toMatchArray([
            'type' => 'quorum',
        ]);
        expect($queue['arguments']['x-delivery-limit'] ?? null)->toBe(-1);
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});

test('creates nothing when preflight finds an incompatible declared resource', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create();
    $broker->configureApplication();
    $missingTopic = 'orders_'.bin2hex(random_bytes(4));
    $incompatibleTopic = 'returns_'.bin2hex(random_bytes(4));
    $broker->declareExchange($incompatibleTopic, 'direct');

    Spoolrail::subscribe(
        $missingTopic,
        'warehouse_'.bin2hex(random_bytes(4)),
        NoopMessageHandler::class,
    );
    Spoolrail::subscribe(
        $incompatibleTopic,
        'returns_'.bin2hex(random_bytes(4)),
        NoopMessageHandler::class,
    );

    try {
        // --- Act ---
        $caught = null;

        try {
            $this->artisan('spoolrail:sync')->run();
        } catch (Throwable $exception) {
            $caught = $exception;
        }

        // --- Assert ---
        expect($caught)->toBeInstanceOf(TopologyPreflightException::class);
        expect($broker->exchange($missingTopic))->toBeNull();
        expect($broker->exchange($incompatibleTopic))->toMatchArray([
            'type' => 'direct',
        ]);
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});
