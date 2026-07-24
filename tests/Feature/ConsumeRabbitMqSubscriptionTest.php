<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageSerializer;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;
use Spoolrail\Spoolrail\Tests\Fixtures\RabbitMqTestBroker;

test('leaves every unacknowledged message available when a prefetch-window handoff fails', function (): void {
    // --- Arrange ---
    $broker = RabbitMqTestBroker::create();
    $broker->configureApplication();
    $topic = 'orders_'.bin2hex(random_bytes(4));
    $subscription = 'warehouse_'.bin2hex(random_bytes(4));
    $queueName = $broker->queueName($subscription);

    config()->set('spoolrail.connections.rabbitmq.prefetch', 3);
    Spoolrail::subscribe($topic, $subscription, NoopMessageHandler::class);

    try {
        $this->artisan('spoolrail:sync')->run();
        foreach (['first', 'second', 'third', 'fourth'] as $reference) {
            Spoolrail::publish(
                $topic,
                Message::make('order.created', ['reference' => $reference]),
            );
        }

        $serializer = new MessageSerializer;
        $handoffs = [];
        $failure = new RuntimeException('Laravel Queue handoff failed.');

        // --- Act ---
        $caught = null;

        try {
            Spoolrail::connection()->consume(
                $subscription,
                function (string $body) use ($serializer, &$handoffs, $failure): void {
                    $reference = $serializer->deserialize($body)->payload['reference'];
                    $handoffs[] = $reference;

                    if ($reference === 'second') {
                        throw $failure;
                    }
                },
            );
        } catch (Throwable $exception) {
            $caught = $exception;
        }

        $remainingReferences = [];

        for ($index = 0; $index < 3; $index++) {
            $body = $broker->message($queueName);
            expect($body)->not->toBeNull();
            $remainingReferences[] = $serializer->deserialize($body)->payload['reference'];
        }

        sort($remainingReferences);

        // --- Assert ---
        expect($caught)->toBe($failure);
        expect($handoffs)->toBe(['first', 'second']);
        expect($remainingReferences)->toBe(['fourth', 'second', 'third']);
        expect($broker->message($queueName))->toBeNull();
    } finally {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $broker->delete();
    }
});
