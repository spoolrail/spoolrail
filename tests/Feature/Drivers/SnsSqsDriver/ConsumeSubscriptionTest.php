<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithSnsSqs;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\TransportContext;

uses(InteractsWithSnsSqs::class);

test('deletes only deliveries whose Queue handoff returns normally', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');
    $this->artisan('spoolrail:sync')->run();

    $queueUrl = $this->sqsQueueUrl('warehouse-orders');
    $this->sqs->setQueueAttributes([
        'QueueUrl' => $queueUrl,
        'Attributes' => ['VisibilityTimeout' => '0'],
    ]);

    $first = Spoolrail::connection('snssqs')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'first']),
        orderingKey: 'order:42',
    );
    $second = Spoolrail::connection('snssqs')->publish(
        'orders',
        Message::make('order.created', ['sequence' => 'second']),
        orderingKey: 'order:42',
    );
    $handoffs = [];
    $failure = new RuntimeException('Stop on the second handoff.');

    // --- Act ---
    try {
        Spoolrail::connection('snssqs')->consume(
            'warehouse-orders',
            function (string $body, TransportContext $context) use (&$handoffs, $failure): void {
                $handoffs[] = [$body, $context];

                if (count($handoffs) === 2) {
                    throw $failure;
                }
            },
        );
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    $remaining = $this->sqs->receiveMessage([
        'QueueUrl' => $queueUrl,
        'MaxNumberOfMessages' => 1,
        'WaitTimeSeconds' => 1,
        'AttributeNames' => ['All'],
    ])->get('Messages');

    expect($caught)->toBe($failure);
    expect($handoffs)->toHaveCount(2);
    expect(json_decode($handoffs[0][0], true, flags: JSON_THROW_ON_ERROR)['id'])->toBe($first->id);
    expect(json_decode($handoffs[1][0], true, flags: JSON_THROW_ON_ERROR)['id'])->toBe($second->id);
    expect($handoffs[0][1]->topic)->toBe('orders');
    expect($handoffs[0][1]->orderingKey)->toBe('order:42');
    expect(json_decode($remaining[0]['Body'], true, flags: JSON_THROW_ON_ERROR)['id'])->toBe($second->id);
});
