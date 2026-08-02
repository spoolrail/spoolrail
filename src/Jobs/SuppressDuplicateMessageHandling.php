<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Jobs;

use Closure;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Contracts\Config\Repository as Config;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;

class SuppressDuplicateMessageHandling
{
    public function __construct(
        private readonly CacheFactory $cacheStores,
        private readonly Config $config,
    ) {}

    public function handle(HandleMessageJob $job, Closure $next): void
    {
        if (! $this->enabled()) {
            $next($job);

            return;
        }

        $cache = $this->cache();
        $key = 'spoolrail:handled:'.hash('xxh128', "$job->subscription:{$job->message->id}");
        $lock = $this->lockProvider($cache)->lock("$key:lock", $this->lockSeconds($job));

        if (! $lock->get()) {
            $job->release(10);

            return;
        }

        try {
            if ($cache->has($key)) {
                return;
            }

            $next($job);

            if ($this->handledToCompletion($job)) {
                $cache->put($key, true, $this->remember());
            }
        } finally {
            $lock->release();
        }
    }

    public function ensureStoreSupportsLocks(): void
    {
        if ($this->enabled()) {
            $this->lockProvider($this->cache());
        }
    }

    /**
     * Handler middleware may release or fail the job without an exception,
     * leaving the handler unrun; remembering such an attempt would skip
     * the retry and lose the message.
     */
    private function handledToCompletion(HandleMessageJob $job): bool
    {
        return $job->job === null
            || (! $job->job->isReleased() && ! $job->job->hasFailed());
    }

    private function lockProvider(Cache $cache): LockProvider
    {
        $store = $cache->getStore();

        if (! $store instanceof LockProvider) {
            throw InvalidConfigException::deduplicationStore($this->storeName());
        }

        return $store;
    }

    /**
     * The lock must outlive the slowest legitimate handler run, yet still
     * expire so a crashed worker cannot block the redelivered message
     * forever. A handler that outruns the lock loses concurrent duplicate
     * suppression for that run; the completion marker still suppresses
     * later duplicates, and no path loses the message.
     */
    private function lockSeconds(HandleMessageJob $job): int
    {
        /** @var int|numeric-string $seconds */
        $seconds = $this->config->get('spoolrail.deduplication.lock', 300);
        $timeout = is_numeric($job->timeout) ? (int) $job->timeout : 0;

        return max((int) $seconds, $timeout + 60);
    }

    private function enabled(): bool
    {
        return $this->config->get('spoolrail.deduplication.enabled', true) === true;
    }

    private function cache(): Cache
    {
        return $this->cacheStores->store($this->storeName());
    }

    private function storeName(): ?string
    {
        /** @var string|null $store */
        $store = $this->config->get('spoolrail.deduplication.store');

        return $store;
    }

    private function remember(): int
    {
        /** @var int|numeric-string $seconds */
        $seconds = $this->config->get('spoolrail.deduplication.remember', 86400);

        return (int) $seconds;
    }
}
