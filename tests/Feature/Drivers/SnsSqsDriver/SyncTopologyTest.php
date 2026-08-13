<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\SnsSqsTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithSnsSqs;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithSnsSqs::class);

test('synchronizes high-throughput FIFO topology and raw fanout', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');

    // --- Act ---
    $firstSync = $this->artisan('spoolrail:sync')->run();
    $published = Spoolrail::connection('snssqs')->publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
        ['correlation-id' => 'A-42'],
        orderingKey: 'order:42',
    );
    $secondSync = $this->artisan('spoolrail:sync')->run();

    // --- Assert ---
    $topic = $this->snsTopicAttributes('orders');
    $queue = $this->sqsQueueAttributes('warehouse-orders');
    $delivery = $this->sqs->receiveMessage([
        'QueueUrl' => $this->sqsQueueUrl('warehouse-orders'),
        'MaxNumberOfMessages' => 1,
        'WaitTimeSeconds' => 1,
        'AttributeNames' => ['All'],
        'MessageAttributeNames' => ['All'],
    ])->get('Messages')[0];

    expect($firstSync)->toBe(0);
    expect($secondSync)->toBe(0);
    expect($topic)->toMatchArray([
        'FifoTopic' => 'true',
        'FifoThroughputScope' => 'MessageGroup',
    ]);
    expect($queue)->toMatchArray([
        'FifoQueue' => 'true',
        'DeduplicationScope' => 'messageGroup',
        'FifoThroughputLimit' => 'perMessageGroupId',
    ]);
    expect((new MessageEnvelope)->decode($delivery['Body']))->toEqual($published);
    expect($delivery['Attributes']['MessageGroupId'])->toBe('order:42');
    expect($delivery['MessageAttributes']['correlation-id']['StringValue'])->toBe('A-42');
});

test('synchronizes standard topology and propagates a supplied fair-queue group', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.snssqs.fifo', false);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');

    // --- Act ---
    $this->artisan('spoolrail:sync')->run();
    Spoolrail::connection('snssqs')->publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
        orderingKey: 'tenant:42',
    );

    // --- Assert ---
    $delivery = $this->sqs->receiveMessage([
        'QueueUrl' => $this->sqsQueueUrl('warehouse-orders', false),
        'MaxNumberOfMessages' => 1,
        'WaitTimeSeconds' => 1,
        'AttributeNames' => ['All'],
    ])->get('Messages')[0];

    expect($this->snsTopicAttributes('orders', false)['FifoTopic'] ?? 'false')->toBe('false');
    expect($delivery['Attributes']['MessageGroupId'])->toBe('tenant:42');
});

test('rejects a standard application queue before creating FIFO replacement topology', function (): void {
    // --- Arrange ---
    $standardQueue = $this->sqsQueueName('warehouse-orders', false);
    $this->sqs->createQueue(['QueueName' => $standardQueue]);

    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');

    // --- Act ---
    try {
        $this->artisan('spoolrail:sync')->run();
    } catch (TopologyPreflightException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->failuresByConnection['snssqs'] ?? null)
        ->toBeInstanceOf(SnsSqsTopologyException::class);
    expect($caught?->failuresByConnection['snssqs']->getMessage() ?? '')
        ->toContain('Changing [fifo] selects replacement topology');
    expect($this->sns->listTopics()->get('Topics'))->toBe([]);
});

test('allows standard and FIFO topics to coexist', function (): void {
    // --- Arrange ---
    $this->sns->createTopic(['Name' => 'orders']);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:sync')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->snsTopicAttributes('orders', false))->toBeArray();
    expect($this->snsTopicAttributes('orders'))->toMatchArray([
        'FifoTopic' => 'true',
        'FifoThroughputScope' => 'MessageGroup',
    ]);
});
