<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;
use Spoolrail\Spoolrail\Tests\Fixtures\RabbitMqTestBroker;

test('deletes only undeclared receive resources from the selected application namespace', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create();
    $broker->configureApplication();
    $topic = 'orders_'.bin2hex(random_bytes(4));
    $activeSubscription = 'active_'.bin2hex(random_bytes(4));
    $staleSubscription = 'stale_'.bin2hex(random_bytes(4));
    $activeQueue = $broker->queueName($activeSubscription);
    $staleQueue = $broker->queueName($staleSubscription);

    Spoolrail::subscribe($topic, $activeSubscription, NoopMessageHandler::class);

    try {
        $this->artisan('spoolrail:sync')->run();
        $broker->declareQueue($staleQueue);
        $broker->bind($topic, $staleQueue);
        Spoolrail::publish($topic, Message::make('order.created', ['reference' => 'A-42']));

        // --- Act ---
        $exit = $this->artisan('spoolrail:delete-undeclared-subscriptions')->run();

        // --- Assert ---
        expect($exit)->toBe(0);
        expect($broker->queue($staleQueue))->toBeNull();
        expect($broker->queue($activeQueue))->not->toBeNull();
        expect($broker->exchange($topic))->not->toBeNull();
        expect($broker->message($activeQueue))->not->toBeNull();
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});

test('deletes the retired physical namespace even when the logical subscription remains active', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create();
    $broker->configureApplication();
    $topic = 'orders_'.bin2hex(random_bytes(4));
    $subscription = 'warehouse_'.bin2hex(random_bytes(4));
    $retiredPrefix = 'retired-'.bin2hex(random_bytes(4));
    $retiredQueue = "$retiredPrefix-$subscription";
    $currentQueue = $broker->queueName($subscription);

    Spoolrail::subscribe($topic, $subscription, NoopMessageHandler::class);

    try {
        $this->artisan('spoolrail:sync')->run();
        $broker->declareQueue($retiredQueue);
        $broker->bind($topic, $retiredQueue);

        // --- Act ---
        $exit = $this
            ->artisan("spoolrail:delete-undeclared-subscriptions --retired-prefix=$retiredPrefix")
            ->run();

        // --- Assert ---
        expect($exit)->toBe(0);
        expect($broker->queue($retiredQueue))->toBeNull();
        expect($broker->queue($currentQueue))->not->toBeNull();
        expect($broker->exchange($topic))->not->toBeNull();
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});

test('refuses to delete a topic with a subscription and never cascades to its queue', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create();
    $broker->configureApplication();
    $topic = 'orders_'.bin2hex(random_bytes(4));
    $subscription = 'warehouse_'.bin2hex(random_bytes(4));
    $queueName = $broker->queueName($subscription);

    Spoolrail::subscribe($topic, $subscription, NoopMessageHandler::class);

    try {
        $this->artisan('spoolrail:sync')->run();

        // --- Act ---
        $caught = null;

        try {
            $this->artisan("spoolrail:delete-topic $topic")->run();
        } catch (Throwable $exception) {
            $caught = $exception;
        }

        // --- Assert ---
        expect($caught)->toBeInstanceOf(RabbitMqTopologyException::class);
        expect($broker->exchange($topic))->not->toBeNull();
        expect($broker->queue($queueName))->not->toBeNull();
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});

test('deletes an unused named topic without deleting unrelated receive resources', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create();
    $broker->configureApplication();
    $topic = 'unused_'.bin2hex(random_bytes(4));
    $unrelatedQueue = $broker->prefix.'-unrelated_'.bin2hex(random_bytes(4));
    $broker->declareExchange($topic);
    $broker->declareQueue($unrelatedQueue);

    try {
        // --- Act ---
        $exit = $this->artisan("spoolrail:delete-topic $topic")->run();

        // --- Assert ---
        expect($exit)->toBe(0);
        expect($broker->exchange($topic))->toBeNull();
        expect($broker->queue($unrelatedQueue))->not->toBeNull();
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});
