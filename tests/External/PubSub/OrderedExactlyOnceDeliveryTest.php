<?php

declare(strict_types=1);

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithDatabaseQueue;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithExternalPubSub;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithDatabaseQueue::class, InteractsWithExternalPubSub::class);

test('hands off ordered deliveries through exactly-once settlement', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe(
        $this->externalTopic,
        $this->externalSubscription,
        RecordingMessageHandler::class,
    )->onConnection('pubsub');

    // --- Act ---
    $sync = $this->artisan('spoolrail:ensure-topology')->run();
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
    $failures = [];
    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldReceive('report')->andReturnUsing(
        static function (Throwable $exception) use (&$failures): void {
            $failures[] = $exception;
        },
    );
    app()->instance(ExceptionHandler::class, $exceptions);
    $consumer = app(SubscriptionConsumer::class);
    $queuedJobs = [];
    Event::listen(JobQueued::class, static function (JobQueued $event) use ($consumer, &$queuedJobs): void {
        $queuedJobs[] = $event->job;

        if (count($queuedJobs) === 2) {
            $consumer->stop();
        }
    });
    $this->runExternalOperationWithin(90, function () use ($consumer): void {
        $consumer->consume([$this->externalSubscription]);
    });

    // --- Assert ---
    expect($sync)->toBe(0);
    expect($subscriptionInfo)->toMatchArray([
        'enableMessageOrdering' => true,
        'enableExactlyOnceDelivery' => true,
    ]);
    expect($failures)->toBe([]);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(2);
    expect(array_map(
        static fn (mixed $job): ?string => $job instanceof HandleMessageJob
            ? $job->message->id
            : null,
        $queuedJobs,
    ))->toBe([$first->id, $second->id]);
});
