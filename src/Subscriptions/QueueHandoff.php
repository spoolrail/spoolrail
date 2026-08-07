<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Illuminate\Cache\Lock;
use Illuminate\Cache\NullStore;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Contracts\Config\Repository as Config;
use Illuminate\Contracts\Queue\Queue;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\QueueHandoffException;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Jobs\HandlerQueuePolicy;
use Spoolrail\Spoolrail\Message;

class QueueHandoff
{
    private const string COMPLETION_LOCK_OWNER = 'spoolrail:completed';

    private const int COMPLETION_PROBE_SECONDS = 1;

    public function __construct(
        private HandlerQueuePolicy $handlerQueuePolicy,
        private CacheFactory $cacheStores,
        private Config $config,
    ) {}

    public function ensureConfigured(): void
    {
        $lockProvider = $this->lockProvider($this->cache());

        $this->newLock($lockProvider, 'spoolrail:handoff:configuration', $this->expiry());
    }

    public function push(Subscription $subscription, Message $message, Queue $queue): void
    {
        $lockProvider = $this->lockProvider($this->cache());
        $handoffKey = $this->handoffKey($subscription, $message);
        $expiry = $this->expiry();
        $attemptLock = $this->newLock($lockProvider, "$handoffKey:attempt", $expiry);

        if (! $attemptLock->get()) {
            throw QueueHandoffException::alreadyInProgress($subscription->name(), $message->id);
        }

        try {
            if ($this->wasRecentlyHandedOff($lockProvider, $handoffKey, $expiry, $subscription, $message)) {
                return;
            }

            $this->pushJob($subscription, $message, $queue);
            $this->retainCompletionLock($lockProvider, $handoffKey, $expiry, $subscription, $message);
        } finally {
            $attemptLock->release();
        }
    }

    private function wasRecentlyHandedOff(
        LockProvider $lockProvider,
        string $handoffKey,
        int $expiry,
        Subscription $subscription,
        Message $message,
    ): bool {
        $probeLock = $this->newLock($lockProvider, "$handoffKey:completed", self::COMPLETION_PROBE_SECONDS);

        if ($probeLock->get()) {
            $probeLock->release();

            return false;
        }

        $completionLock = $this->newLock($lockProvider, "$handoffKey:completed", $expiry, self::COMPLETION_LOCK_OWNER);

        if ($completionLock->isOwnedByCurrentProcess()) {
            return true;
        }

        throw QueueHandoffException::alreadyInProgress($subscription->name(), $message->id);
    }

    private function retainCompletionLock(
        LockProvider $lockProvider,
        string $handoffKey,
        int $expiry,
        Subscription $subscription,
        Message $message,
    ): void {
        $completionLock = $this->newLock($lockProvider, "$handoffKey:completed", $expiry, self::COMPLETION_LOCK_OWNER);

        if (! $completionLock->get()) {
            throw QueueHandoffException::couldNotRetainCompletionLock($subscription->name(), $message->id);
        }
    }

    private function pushJob(Subscription $subscription, Message $message, Queue $queue): void
    {
        $job = new HandleMessageJob($message, $subscription->name());

        $this->handlerQueuePolicy->apply($subscription->handlerClass(), $message, $job);

        $queue->push($job, '', $subscription->queueName());
    }

    private function newLock(
        LockProvider $lockProvider,
        string $name,
        int $seconds,
        ?string $owner = null,
    ): Lock {
        $lock = $lockProvider->lock($name, $seconds, $owner);

        if (! $lock instanceof Lock) {
            throw InvalidConfigException::unsupportedHandoffIdempotencyCacheStore($this->cacheStoreName());
        }

        return $lock;
    }

    private function cache(): Cache
    {
        return $this->cacheStores->store($this->cacheStoreName());
    }

    private function lockProvider(Cache $cache): LockProvider
    {
        $store = $cache->getStore();

        if ($store instanceof NullStore || ! $store instanceof LockProvider) {
            throw InvalidConfigException::unsupportedHandoffIdempotencyCacheStore($this->cacheStoreName());
        }

        return $store;
    }

    private function cacheStoreName(): ?string
    {
        /** @var string|null $store */
        $store = $this->config->get('spoolrail.handoff_idempotency.cache_store');

        return $store;
    }

    private function expiry(): int
    {
        $expiry = $this->config->get('spoolrail.handoff_idempotency.expiry', 600);

        if (! is_int($expiry) || $expiry < 1) {
            throw InvalidConfigException::invalidHandoffIdempotencyExpiry();
        }

        return $expiry;
    }

    private function handoffKey(Subscription $subscription, Message $message): string
    {
        return 'spoolrail:handoff:'.hash('xxh128', "{$subscription->name()}:$message->id");
    }
}
