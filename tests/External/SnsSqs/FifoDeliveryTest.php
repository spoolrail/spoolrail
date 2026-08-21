<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithExternalSnsSqs;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\TransportContext;

uses(InteractsWithExternalSnsSqs::class);

test('preserves FIFO order while suppressing a repeated publication', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe(
        $this->externalTopic,
        $this->externalSubscription,
        RecordingMessageHandler::class,
    )->onConnection('snssqs');
    $firstMessage = Message::make('order.created', ['sequence' => 'first']);
    $secondMessage = Message::make('order.created', ['sequence' => 'second']);

    // --- Act ---
    $sync = $this->artisan('spoolrail:ensure-topology')->run();
    $connection = Spoolrail::connection('snssqs');
    $first = $connection->publish(
        $this->externalTopic,
        $firstMessage,
        orderingKey: 'order:42',
    );
    $connection->publish(
        $this->externalTopic,
        $firstMessage,
        orderingKey: 'order:42',
    );
    $second = $connection->publish(
        $this->externalTopic,
        $secondMessage,
        orderingKey: 'order:42',
    );
    $topicAttributes = $this->externalSnsTopicAttributes();
    $queueAttributes = $this->externalSqsQueueAttributes();
    $deliveries = [];
    $finished = new RuntimeException('The expected AWS deliveries arrived.');
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
    expect($topicAttributes)->toMatchArray([
        'FifoTopic' => 'true',
        'FifoThroughputScope' => 'MessageGroup',
    ]);
    expect($queueAttributes)->toMatchArray([
        'FifoQueue' => 'true',
        'DeduplicationScope' => 'messageGroup',
        'FifoThroughputLimit' => 'perMessageGroupId',
    ]);
    expect($caught)->toBe($finished);
    expect(array_map(
        static fn (Message $message): string => $message->id,
        $deliveries,
    ))->toBe([$first->id, $second->id]);
});
