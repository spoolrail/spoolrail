<?php

declare(strict_types=1);

use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithDatabaseQueue;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithSnsSqs;
use Spoolrail\Spoolrail\Tests\Concerns\RecordsConsumerFailures;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(
    InteractsWithDatabaseQueue::class,
    InteractsWithSnsSqs::class,
    RecordsConsumerFailures::class,
);

test('processes two SQS subscriptions through the sync queue in one shared runtime', function (): void {
    // --- Arrange ---
    config()->set('queue.default', 'sync');
    RecordingMessageHandler::reset();
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');
    $this->artisan('spoolrail:ensure-topology')->run();
    $consumer = app(SubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        if (count(RecordingMessageHandler::$messages) === 2) {
            $consumer->stop();
        }
    });
    Spoolrail::connection('snssqs')->publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
    );

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    $subscriptions = array_map(
        static fn (Message $message): ?string => $message->transport?->subscription,
        RecordingMessageHandler::$messages,
    );
    sort($subscriptions);
    expect($this->consumerFailures)->toBe([]);
    expect($subscriptions)->toBe(['billing-orders', 'warehouse-orders']);
});

test('queues two SQS subscriptions through the database queue in one shared runtime', function (): void {
    // --- Arrange ---
    RecordingMessageHandler::reset();
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');
    $this->artisan('spoolrail:ensure-topology')->run();
    $consumer = app(SubscriptionConsumer::class);
    $queuedJobs = [];
    Event::listen(JobQueued::class, static function (JobQueued $event) use ($consumer, &$queuedJobs): void {
        $queuedJobs[] = $event->job;

        if (count($queuedJobs) === 2) {
            $consumer->stop();
        }
    });
    Spoolrail::connection('snssqs')->publish(
        'orders',
        Message::make('order.created', ['reference' => 'A-42']),
    );

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    $subscriptions = array_map(
        static fn (mixed $job): ?string => $job instanceof HandleMessageJob
            ? $job->message->transport?->subscription
            : null,
        $queuedJobs,
    );
    sort($subscriptions);
    expect($this->consumerFailures)->toBe([]);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(2);
    expect($subscriptions)->toBe(['billing-orders', 'warehouse-orders']);
});
