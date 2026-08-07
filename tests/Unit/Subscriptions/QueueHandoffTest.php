<?php

declare(strict_types=1);

use Illuminate\Cache\ArrayLock;
use Illuminate\Cache\ArrayStore;
use Illuminate\Contracts\Cache\Lock;
use Illuminate\Contracts\Cache\Repository;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\QueueHandoffException;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\QueueHandoff;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithDatabaseQueue;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

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

test('accepts a handoff after a contended attempt lock expires', function (): void {
    // --- Arrange ---
    $this->createJobsTable();
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
    $this->createJobsTable();
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
    $this->createJobsTable();

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
    $this->createJobsTable();
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
