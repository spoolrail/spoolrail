<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithSnsSqs;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\TransportContext;

uses(InteractsWithSnsSqs::class);

test('leases one SQS batch and settles only deliveries whose handoffs succeed', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.snssqs.fifo', false);
    config()->set('spoolrail.connections.snssqs.receive_batch_size', 3);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');
    $this->artisan('spoolrail:sync')->run();

    $queueUrl = $this->sqsQueueUrl('warehouse-orders', false);
    $this->sqs->setQueueAttributes([
        'QueueUrl' => $queueUrl,
        'Attributes' => ['VisibilityTimeout' => '2'],
    ]);

    $publishedMessages = [
        Spoolrail::connection('snssqs')->publish(
            'orders',
            Message::make('order.created', ['sequence' => 'first']),
        ),
        Spoolrail::connection('snssqs')->publish(
            'orders',
            Message::make('order.created', ['sequence' => 'second']),
        ),
        Spoolrail::connection('snssqs')->publish(
            'orders',
            Message::make('order.created', ['sequence' => 'third']),
        ),
    ];
    $availableCount = 0;
    $availabilityDeadline = microtime(true) + 3;

    do {
        $availableCount = (int) ($this->sqs->getQueueAttributes([
            'QueueUrl' => $queueUrl,
            'AttributeNames' => ['ApproximateNumberOfMessages'],
        ])->get('Attributes')['ApproximateNumberOfMessages'] ?? 0);

        if ($availableCount < 3) {
            usleep(20_000);
        }
    } while ($availableCount < 3 && microtime(true) < $availabilityDeadline);

    expect($availableCount)->toBe(3);

    $handedOffIds = [];
    $failure = new RuntimeException('Stop on the second handoff.');
    $caught = null;

    // --- Act ---
    try {
        Spoolrail::connection('snssqs')->consume(
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

    $deliveriesAvailableDuringLease = [];
    $leaseObservationDeadline = microtime(true) + 0.5;

    do {
        $deliveriesAvailableDuringLease = $this->sqs->receiveMessage([
            'QueueUrl' => $queueUrl,
            'MaxNumberOfMessages' => 10,
            'WaitTimeSeconds' => 0,
            'AttributeNames' => ['All'],
        ])->get('Messages') ?? [];

        if ($deliveriesAvailableDuringLease === []) {
            usleep(20_000);
        }
    } while ($deliveriesAvailableDuringLease === [] && microtime(true) < $leaseObservationDeadline);

    sleep(3);

    $redeliveries = [];
    $redeliveryDeadline = microtime(true) + 1;

    do {
        foreach ($this->sqs->receiveMessage([
            'QueueUrl' => $queueUrl,
            'MaxNumberOfMessages' => 3 - count($redeliveries),
            'WaitTimeSeconds' => 0,
            'AttributeNames' => ['All'],
        ])->get('Messages') ?? [] as $delivery) {
            $id = json_decode($delivery['Body'], true, flags: JSON_THROW_ON_ERROR)['id'];
            $redeliveries[$id] = $delivery;
        }

        if (count($redeliveries) < 3) {
            usleep(20_000);
        }
    } while (count($redeliveries) < 3 && microtime(true) < $redeliveryDeadline);

    // --- Assert ---
    expect($caught)->toBe($failure);
    expect($handedOffIds)->toHaveCount(2);
    expect($deliveriesAvailableDuringLease)->toBe([]);

    $publishedIds = array_map(static fn (Message $message): string => $message->id, $publishedMessages);
    $expectedRedeliveryIds = array_values(array_diff($publishedIds, [$handedOffIds[0]]));
    $redeliveredIds = array_keys($redeliveries);
    sort($expectedRedeliveryIds);
    sort($redeliveredIds);

    expect($redeliveredIds)->toBe($expectedRedeliveryIds);
});
