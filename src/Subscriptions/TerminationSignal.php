<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\NullStore;
use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Contracts\Config\Repository as Config;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;

class TerminationSignal
{
    private string $hostname;

    public function __construct(
        private Cache $cache,
        private Config $config,
        ?string $hostname = null,
    ) {
        $this->hostname = $hostname ?? gethostname() ?: php_uname('n');
    }

    public function current(): ?string
    {
        $this->ensureCacheIsSupported();

        $generation = $this->cache->get($this->key());

        return is_string($generation) ? $generation : null;
    }

    public function broadcast(): void
    {
        $this->ensureCacheIsSupported();

        if (! $this->cache->forever($this->key(), bin2hex(random_bytes(16)))) {
            throw ConsumerException::terminationSignalWasNotStored();
        }
    }

    private function ensureCacheIsSupported(): void
    {
        $store = $this->cache->getStore();

        if ($store instanceof ArrayStore || $store instanceof NullStore) {
            $name = $this->config->get('cache.default', 'default');

            throw ConsumerException::unsupportedTerminationCacheStore(
                is_string($name) ? $name : 'default',
            );
        }
    }

    private function key(): string
    {
        return "spoolrail:consumer:terminate:$this->hostname";
    }
}
