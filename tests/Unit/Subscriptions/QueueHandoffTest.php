<?php

declare(strict_types=1);

use Illuminate\Cache\ArrayLock;
use Illuminate\Cache\ArrayStore;
use Illuminate\Contracts\Cache\Lock;
use Illuminate\Contracts\Cache\Repository;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Spoolrail\Spoolrail\Events\MessageConsumed;
use Spoolrail\Spoolrail\Events\MessageConsuming;
use Spoolrail\Spoolrail\Events\MessageConsumptionFailed;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\QueueHandoffException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\QueueHandoff;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithDatabaseQueue;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\TransportContext;

uses(InteractsWithDatabaseQueue::class);

beforeEach(function (): void {
    RecordingMessageHandler::reset();
});

test('rejects a cache store without atomic locks', function (): void {
    $locklessStore = Mockery::mock(Store::class);

    Cache::extend('lockless', fn (): Repository => Cache::repository($locklessStore));
    config()->set('cache.stores.lockless', ['driver' => 'lockless']);
    config()->set('spoolrail.handoff_idempotency.cache_store', 'lockless');

    expect(fn () => app(QueueHandoff::class)->ensureConfigured())
        ->toThrow(
            InvalidConfigException::class,
            'Spoolrail Queue handoff idempotency requires a cache store backed by Laravel atomic locks, and the [lockless] cache store is not supported. Configure [spoolrail.handoff_idempotency.cache_store] with a supported store.',
        );
});

test('rejects the non-persistent null cache driver', function (): void {
    config()->set('cache.stores.discard_handoffs', ['driver' => 'null']);
    config()->set('spoolrail.handoff_idempotency.cache_store', 'discard_handoffs');

    expect(fn () => app(QueueHandoff::class)->ensureConfigured())
        ->toThrow(InvalidConfigException::class);
});

test('rejects cache locks without ownership inspection', function (): void {
    $store = Mockery::mock(ArrayStore::class)->makePartial();
    $store->shouldReceive('lock')->andReturn(Mockery::mock(Lock::class));

    Cache::extend('contract_only_locks', fn (): Repository => Cache::repository($store));
    config()->set('cache.stores.contract_only_locks', ['driver' => 'contract_only_locks']);
    config()->set('spoolrail.handoff_idempotency.cache_store', 'contract_only_locks');

    expect(fn () => app(QueueHandoff::class)->ensureConfigured())
        ->toThrow(InvalidConfigException::class);
});

test('rejects a non-positive handoff idempotency expiry', function (): void {
    config()->set('spoolrail.handoff_idempotency.expiry', 0);

    expect(fn () => app(QueueHandoff::class)->ensureConfigured())
        ->toThrow(
            InvalidConfigException::class,
            'Spoolrail Queue handoff idempotency expiry must be a positive integer.',
        );
});

test('allows the same message to be handed off after Laravel Queue rejects it', function (): void {
    // --- Arrange ---
    $failure = new RuntimeException('Laravel Queue is unavailable.');
    $queue = Mockery::mock(Queue::class);
    $queue->expects('push')->once()->ordered()->andThrow($failure);
    $queue->expects('push')->once()->ordered()->andReturn('queued');

    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $message = Message::make('order.created', []);
    $handoff = app(QueueHandoff::class);

    // --- Act ---
    $caught = null;

    try {
        $handoff->push($subscription, $message, $queue);
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    $handoff->push($subscription, $message, $queue);

    // --- Assert ---
    expect($caught)->toBe($failure);
});

test('observes a queue push after suppression checks and contains successful observers', function (): void {
    // --- Arrange ---
    $timeline = [];
    $observed = [];
    $queue = Mockery::mock(Queue::class);
    $queue->expects('push')
        ->once()
        ->andReturnUsing(static function () use (&$timeline): string {
            $timeline[] = 'queue push';

            return 'queued';
        });

    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $transport = new TransportContext(
        driver: 'rabbitmq',
        connectionName: 'events',
        topic: 'orders',
        subscription: 'warehouse-order-processing',
        headers: ['traceparent' => '00-trace-span-01'],
    );
    $message = Message::make('order.created', [])->withTransport($transport);

    Event::listen(MessageConsuming::class, static function (MessageConsuming $event) use (&$timeline, &$observed): void {
        $timeline[] = MessageConsuming::class;
        $observed[] = $event;
    });
    Event::listen(MessageConsuming::class, static function (): never {
        throw new RuntimeException('Before observer failed.');
    });
    Event::listen(MessageConsumed::class, static function (MessageConsumed $event) use (&$timeline, &$observed): void {
        $timeline[] = MessageConsumed::class;
        $observed[] = $event;
    });
    Event::listen(MessageConsumed::class, static function (): never {
        throw new RuntimeException('Terminal observer failed.');
    });

    // --- Act ---
    app(QueueHandoff::class)->push($subscription, $message, $queue);

    // --- Assert ---
    expect($timeline)->toBe([
        MessageConsuming::class,
        'queue push',
        MessageConsumed::class,
    ]);
    expect($observed[0]->message)->toBe($message);
    expect($observed[0]->message->transport)->toBe($transport);
    expect($observed[1]->message)->toBe($message);
});

test('reports a queue push failure without allowing its observer to replace it', function (): void {
    // --- Arrange ---
    $events = [];
    $failure = new RuntimeException('Laravel Queue is unavailable.');
    $queue = Mockery::mock(Queue::class);
    $queue->expects('push')->once()->andThrow($failure);
    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $message = Message::make('order.created', []);

    Event::listen(MessageConsuming::class, static function (MessageConsuming $event) use (&$events): void {
        $events[] = $event;
    });
    Event::listen(MessageConsumed::class, static function (MessageConsumed $event) use (&$events): void {
        $events[] = $event;
    });
    Event::listen(MessageConsumptionFailed::class, static function (MessageConsumptionFailed $event) use (&$events): never {
        $events[] = $event;

        throw new RuntimeException('Failure observer failed.');
    });

    // --- Act ---
    $caught = null;

    try {
        app(QueueHandoff::class)->push($subscription, $message, $queue);
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
    expect(array_map(static fn (object $event): string => $event::class, $events))->toBe([
        MessageConsuming::class,
        MessageConsumptionFailed::class,
    ]);
    expect($events[1]->exception)->toBe($failure);
});

test('reports a job preparation failure before pushing to Laravel Queue', function (): void {
    // --- Arrange ---
    RecordingMessageHandler::$queuePolicyFailuresRemaining = 1;

    $events = [];
    $queue = Mockery::mock(Queue::class);
    $queue->shouldNotReceive('push');
    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $message = Message::make('order.created', []);

    foreach ([MessageConsuming::class, MessageConsumed::class, MessageConsumptionFailed::class] as $eventClass) {
        Event::listen($eventClass, static function (object $event) use (&$events): void {
            $events[] = $event;
        });
    }

    // --- Act ---
    $caught = null;

    try {
        app(QueueHandoff::class)->push($subscription, $message, $queue);
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBeInstanceOf(RuntimeException::class);
    expect($caught?->getMessage())->toBe('Handler queue policy failed.');
    expect(array_map(static fn (object $event): string => $event::class, $events))->toBe([
        MessageConsuming::class,
        MessageConsumptionFailed::class,
    ]);
    expect($events[1]->message)->toBe($message);
    expect($events[1]->exception)->toBe($caught);
});

test('emits no consumption lifecycle for a recently completed handoff', function (): void {
    // --- Arrange ---
    $events = [];
    $queue = Mockery::mock(Queue::class);
    $queue->expects('push')->once()->andReturn('queued');
    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $message = Message::make('order.created', []);
    $handoff = app(QueueHandoff::class);

    foreach ([MessageConsuming::class, MessageConsumed::class, MessageConsumptionFailed::class] as $eventClass) {
        Event::listen($eventClass, static function (object $event) use (&$events): void {
            $events[] = $event;
        });
    }

    $handoff->push($subscription, $message, $queue);
    $events = [];

    // --- Act ---
    $handoff->push($subscription, $message, $queue);

    // --- Assert ---
    expect($events)->toBe([]);
});

test('does not reclassify a queued message when completion bookkeeping fails', function (): void {
    // --- Arrange ---
    $completionFailuresRemaining = 1;
    $store = Mockery::mock(ArrayStore::class)->makePartial();
    $store->shouldReceive('lock')->andReturnUsing(
        function (string $name, int $seconds, ?string $owner = null) use ($store, &$completionFailuresRemaining): ArrayLock {
            if (str_ends_with($name, ':completed') && $owner !== null && $completionFailuresRemaining > 0) {
                $completionFailuresRemaining--;
                new ArrayLock($store, $name, $seconds, 'interrupted-handoff')->get();
            }

            return new ArrayLock($store, $name, $seconds, $owner);
        },
    );

    Cache::extend('failed_handoff_completion', fn (): Repository => Cache::repository($store));
    config()->set('cache.stores.failed_handoff_completion', ['driver' => 'failed_handoff_completion']);
    config()->set('spoolrail.handoff_idempotency.cache_store', 'failed_handoff_completion');

    $events = [];
    $queue = Mockery::mock(Queue::class);
    $queue->expects('push')->once()->andReturn('queued');
    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $message = Message::make('order.created', []);

    foreach ([MessageConsuming::class, MessageConsumed::class, MessageConsumptionFailed::class] as $eventClass) {
        Event::listen($eventClass, static function (object $event) use (&$events): void {
            $events[] = $event;
        });
    }

    // --- Act ---
    $caught = null;

    try {
        app(QueueHandoff::class)->push($subscription, $message, $queue);
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBeInstanceOf(QueueHandoffException::class);
    expect(array_map(static fn (object $event): string => $event::class, $events))->toBe([
        MessageConsuming::class,
        MessageConsumed::class,
    ]);
});

test('accepts a handoff after a contended attempt lock expires', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.handoff_idempotency.expiry', 60);

    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $message = Message::make('order.created', []);
    $queue = app(QueueFactory::class)->connection('database');
    $handoff = app(QueueHandoff::class);

    Cache::lock(queueHandoffKey($message, $subscription->name()).':attempt', 60)->get();

    // --- Act ---
    $failure = null;

    try {
        $handoff->push($subscription, $message, $queue);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $queuedWhileLocked = DB::connection('testing')->table('jobs')->count();

    $this->travel(61)->seconds();
    $handoff->push($subscription, $message, $queue);

    // --- Assert ---
    expect($failure)->toBeInstanceOf(QueueHandoffException::class);
    expect($queuedWhileLocked)->toBe(0);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(1);
});

test('queues the message again after its completion lock expires', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.handoff_idempotency.expiry', 60);

    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $message = Message::make('order.created', []);
    $queue = app(QueueFactory::class)->connection('database');
    $handoff = app(QueueHandoff::class);
    $handoff->push($subscription, $message, $queue);

    $this->travel(61)->seconds();

    // --- Act ---
    $handoff->push($subscription, $message, $queue);

    // --- Assert ---
    expect(DB::connection('testing')->table('jobs')->count())->toBe(2);
});

test('rejects a completion lock with an unknown owner', function (): void {
    // --- Arrange ---
    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $message = Message::make('order.created', []);
    $queue = app(QueueFactory::class)->connection('database');
    $handoff = app(QueueHandoff::class);

    $lock = Cache::lock(queueHandoffKey($message, $subscription->name()).':completed', 60);
    $lock->get();

    // --- Act ---
    $failure = null;

    try {
        $handoff->push($subscription, $message, $queue);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $queuedWhileUncertain = DB::connection('testing')->table('jobs')->count();

    $lock->release();
    $handoff->push($subscription, $message, $queue);

    // --- Assert ---
    expect($failure)->toBeInstanceOf(QueueHandoffException::class);
    expect($queuedWhileUncertain)->toBe(0);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(1);
});

test('allows another handoff after a failed completion lock expires', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.handoff_idempotency.expiry', 60);

    $completionFailuresRemaining = 1;
    $store = Mockery::mock(ArrayStore::class)->makePartial();
    $store->shouldReceive('lock')->andReturnUsing(
        function (string $name, int $seconds, ?string $owner = null) use ($store, &$completionFailuresRemaining): ArrayLock {
            if (str_ends_with($name, ':completed') && $owner !== null && $completionFailuresRemaining > 0) {
                $completionFailuresRemaining--;
                new ArrayLock($store, $name, $seconds, 'interrupted-handoff')->get();
            }

            return new ArrayLock($store, $name, $seconds, $owner);
        },
    );

    Cache::extend('failed_handoff_completion', fn (): Repository => Cache::repository($store));
    config()->set('cache.stores.failed_handoff_completion', ['driver' => 'failed_handoff_completion']);
    config()->set('spoolrail.handoff_idempotency.cache_store', 'failed_handoff_completion');

    $subscription = (new SubscriptionRegistry)
        ->subscribe('orders', 'warehouse-order-processing', RecordingMessageHandler::class);
    $message = Message::make('order.created', []);
    $queue = app(QueueFactory::class)->connection('database');
    $handoff = app(QueueHandoff::class);

    // --- Act ---
    $failure = null;

    try {
        $handoff->push($subscription, $message, $queue);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $queuedAfterFailedCompletion = DB::connection('testing')->table('jobs')->count();

    $this->travel(61)->seconds();
    $handoff->push($subscription, $message, $queue);

    // --- Assert ---
    expect($failure)->toBeInstanceOf(QueueHandoffException::class);
    expect($queuedAfterFailedCompletion)->toBe(1);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(2);
});

function queueHandoffKey(Message $message, string $subscription): string
{
    return 'spoolrail:handoff:'.hash('xxh128', "$subscription:$message->id");
}
