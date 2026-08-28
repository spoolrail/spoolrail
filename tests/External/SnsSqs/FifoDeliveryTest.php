<?php

declare(strict_types=1);

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Spoolrail\Spoolrail\Contracts\CanWaitForConsumerIo;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithDatabaseQueue;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithExternalSnsSqs;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithDatabaseQueue::class, InteractsWithExternalSnsSqs::class);

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
    expect($topicAttributes)->toMatchArray([
        'FifoTopic' => 'true',
        'FifoThroughputScope' => 'MessageGroup',
    ]);
    expect($queueAttributes)->toMatchArray([
        'FifoQueue' => 'true',
        'DeduplicationScope' => 'messageGroup',
        'FifoThroughputLimit' => 'perMessageGroupId',
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

test('releases an SQS delivery for immediate redelivery', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe(
        $this->externalTopic,
        $this->externalSubscription,
        RecordingMessageHandler::class,
    )->onConnection('snssqs');
    $this->artisan('spoolrail:ensure-topology')->run();
    $connection = Spoolrail::connection('snssqs');
    $published = $connection->publish(
        $this->externalTopic,
        Message::make('order.created', ['sequence' => 'released']),
        orderingKey: 'order:42',
    );
    $driver = $connection->consumerDriver();

    expect($driver)->toBeInstanceOf(CanWaitForConsumerIo::class);

    // --- Act ---
    $first = null;
    $this->runExternalOperationWithin(90, function () use ($driver, &$first): void {
        $receiving = false;

        while (! $first instanceof Delivery) {
            if (! $receiving) {
                $receiving = true;
                $driver->receive($this->externalSubscription, function (array $deliveries) use (&$first, &$receiving): void {
                    $first = $deliveries[0] ?? null;
                    $receiving = false;
                }, static function (Throwable $exception): never {
                    throw $exception;
                });
            }

            $driver->waitForConsumerIo();
        }
    });
    $released = false;
    $driver->release($first, static function () use (&$released): void {
        $released = true;
    }, static function (Throwable $exception): never {
        throw $exception;
    });
    $this->runExternalOperationWithin(90, function () use ($driver, &$released): void {
        while (! $released) {
            $driver->waitForConsumerIo();
        }
    });

    $redelivery = null;
    $this->runExternalOperationWithin(90, function () use ($driver, &$redelivery): void {
        $receiving = false;

        while (! $redelivery instanceof Delivery) {
            if (! $receiving) {
                $receiving = true;
                $driver->receive($this->externalSubscription, function (array $deliveries) use (&$redelivery, &$receiving): void {
                    $redelivery = $deliveries[0] ?? null;
                    $receiving = false;
                }, static function (Throwable $exception): never {
                    throw $exception;
                });
            }

            $driver->waitForConsumerIo();
        }
    });
    $acknowledged = false;
    $driver->acknowledge($redelivery, static function () use (&$acknowledged): void {
        $acknowledged = true;
    }, static function (Throwable $exception): never {
        throw $exception;
    });
    $this->runExternalOperationWithin(90, function () use ($driver, &$acknowledged): void {
        while (! $acknowledged) {
            $driver->waitForConsumerIo();
        }
    });

    // --- Assert ---
    expect(json_decode($redelivery->body, true, flags: JSON_THROW_ON_ERROR)['id'])
        ->toBe($published->id);
    expect($redelivery->redelivered)->toBeTrue();
});
