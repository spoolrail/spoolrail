<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithExternalPubSub;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\TransportContext;

uses(InteractsWithExternalPubSub::class);

test('hands off ordered deliveries through exactly-once settlement', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe(
        $this->externalTopic,
        $this->externalSubscription,
        RecordingMessageHandler::class,
    )->onConnection('pubsub');

    // --- Act ---
    $sync = $this->artisan('spoolrail:sync')->run();
    $connection = Spoolrail::connection('pubsub');
    $first = $connection->publish(
        $this->externalTopic,
        Message::make('order.created', ['sequence' => 'first']),
        orderingKey: 'order:42',
    );
    $second = $connection->publish(
        $this->externalTopic,
        Message::make('order.created', ['sequence' => 'second']),
        orderingKey: 'order:42',
    );
    $subscription = $this->externalPubSubSubscription();
    $subscriptionInfo = $subscription->info();
    $deliveries = [];
    $finished = new RuntimeException('The expected Google Pub/Sub deliveries arrived.');
    $caught = null;

    try {
        $this->runExternalOperationWithin(90, function () use ($connection, &$deliveries, $finished): void {
            $connection->consume(
                $this->externalSubscription,
                function (string $body, TransportContext $_context) use (&$deliveries, $finished): void {
                    $deliveries[] = (new MessageEnvelope)->decode($body);

                    if (count($deliveries) === 2) {
                        throw $finished;
                    }
                },
            );
        });
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($sync)->toBe(0);
    expect($subscriptionInfo)->toMatchArray([
        'enableMessageOrdering' => true,
        'enableExactlyOnceDelivery' => true,
    ]);
    expect($caught)->toBe($finished);
    expect(array_map(
        static fn (Message $message): string => $message->id,
        $deliveries,
    ))->toBe([$first->id, $second->id]);
});
