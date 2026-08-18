<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithPubSub;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\TransportContext;

uses(InteractsWithPubSub::class);

test('leases one Pub/Sub batch and settles only deliveries whose handoffs succeed', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.pubsub.message_ordering', false);
    config()->set('spoolrail.connections.pubsub.receive_batch_size', 3);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');
    $this->artisan('spoolrail:sync')->run();

    $publishedMessages = [
        Spoolrail::connection('pubsub')->publish(
            'orders',
            Message::make('order.created', ['sequence' => 'first']),
        ),
        Spoolrail::connection('pubsub')->publish(
            'orders',
            Message::make('order.created', ['sequence' => 'second']),
        ),
        Spoolrail::connection('pubsub')->publish(
            'orders',
            Message::make('order.created', ['sequence' => 'third']),
        ),
    ];
    $subscription = $this->pubSubSubscription('warehouse-orders');
    $subscription->update(['ackDeadlineSeconds' => 10]);
    $stagedMessages = [];
    $availabilityDeadline = microtime(true) + 3;

    do {
        foreach ($subscription->pull([
            'maxMessages' => 3 - count($stagedMessages),
            'returnImmediately' => true,
        ]) as $message) {
            $id = json_decode((string) $message->data(), true, flags: JSON_THROW_ON_ERROR)['id'];
            $stagedMessages[$id] = $message;
        }

        if (count($stagedMessages) < 3) {
            usleep(20_000);
        }
    } while (count($stagedMessages) < 3 && microtime(true) < $availabilityDeadline);

    expect($stagedMessages)->toHaveCount(3);
    $subscription->modifyAckDeadlineBatch(array_values($stagedMessages), 0);
    usleep(100_000);

    $handedOffIds = [];
    $failure = new RuntimeException('Stop on the second handoff.');
    $caught = null;

    // --- Act ---
    try {
        Spoolrail::connection('pubsub')->consume(
            'warehouse-orders',
            function (string $body, TransportContext $_context) use (&$handedOffIds, $failure): void {
                $handedOffIds[] = json_decode($body, true, flags: JSON_THROW_ON_ERROR)['id'];

                if (count($handedOffIds) === 2) {
                    throw $failure;
                }
            },
        );
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    $messagesAvailableDuringLease = [];
    $leaseObservationDeadline = microtime(true) + 0.5;

    do {
        $messagesAvailableDuringLease = $subscription->pull([
            'maxMessages' => 3,
            'returnImmediately' => true,
        ]);

        if ($messagesAvailableDuringLease === []) {
            usleep(20_000);
        }
    } while ($messagesAvailableDuringLease === [] && microtime(true) < $leaseObservationDeadline);

    sleep(11);

    $redeliveries = [];
    $redeliveryDeadline = microtime(true) + 1;

    do {
        foreach ($subscription->pull([
            'maxMessages' => 3 - count($redeliveries),
            'returnImmediately' => true,
        ]) as $message) {
            $id = json_decode((string) $message->data(), true, flags: JSON_THROW_ON_ERROR)['id'];
            $redeliveries[$id] = $message;
        }

        if (count($redeliveries) < 3) {
            usleep(20_000);
        }
    } while (count($redeliveries) < 3 && microtime(true) < $redeliveryDeadline);

    // --- Assert ---
    expect($caught)->toBe($failure);
    expect($handedOffIds)->toHaveCount(2);
    expect($messagesAvailableDuringLease)->toBe([]);

    $publishedIds = array_map(static fn (Message $message): string => $message->id, $publishedMessages);
    $expectedRedeliveryIds = array_values(array_diff($publishedIds, [$handedOffIds[0]]));
    $redeliveredIds = array_keys($redeliveries);
    sort($expectedRedeliveryIds);
    sort($redeliveredIds);

    expect($redeliveredIds)->toBe($expectedRedeliveryIds);
});
