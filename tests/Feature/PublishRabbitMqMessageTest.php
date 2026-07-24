<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageSerializer;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;
use Spoolrail\Spoolrail\Tests\Fixtures\RabbitMqTestBroker;

test('publishes without Management API configuration when the shared topic has no subscriptions', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create();
    $broker->configureDataPlaneApplication();
    $topic = 'events_'.bin2hex(random_bytes(4));
    $broker->declareExchange($topic);
    $message = Message::make('event.recorded', ['reference' => 'A-42']);

    try {
        // --- Act ---
        $published = Spoolrail::publish($topic, $message);

        // --- Assert ---
        expect($published->id)->toBe($message->id);
        expect($published->publishedAt)->not->toBeNull();
        expect($broker->exchange($topic))->not->toBeNull();
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});

test('fans one publication out to every declared subscription', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create();
    $broker->configureApplication();
    $topic = 'orders_'.bin2hex(random_bytes(4));
    $firstSubscription = 'warehouse_'.bin2hex(random_bytes(4));
    $secondSubscription = 'analytics_'.bin2hex(random_bytes(4));

    Spoolrail::subscribe($topic, $firstSubscription, NoopMessageHandler::class);
    Spoolrail::subscribe($topic, $secondSubscription, NoopMessageHandler::class);

    try {
        $this->artisan('spoolrail:sync')->run();

        // --- Act ---
        $published = Spoolrail::publish(
            $topic,
            Message::make('order.created', ['reference' => 'A-42']),
        );
        $firstBody = $broker->message($broker->queueName($firstSubscription));
        $secondBody = $broker->message($broker->queueName($secondSubscription));

        // --- Assert ---
        expect($firstBody)->not->toBeNull();
        expect($secondBody)->not->toBeNull();

        $serializer = new MessageSerializer;
        $first = $serializer->deserialize($firstBody);
        $second = $serializer->deserialize($secondBody);

        expect($first)->toEqual($published);
        expect($second)->toEqual($published);
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});
