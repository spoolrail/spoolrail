<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Concerns;

use Closure;
use Dotenv\Dotenv;
use RuntimeException;

trait InteractsWithExternalServices
{
    protected string $externalPrefix;

    protected string $externalResourceStem;

    protected string $externalSubscription;

    protected string $externalTopic;

    protected function setUpExternalTestEnvironment(): void
    {
        $path = dirname(__DIR__, 2);
        $environment = "$path/.env.external";

        if (! is_file($environment)) {
            throw new RuntimeException(
                'External tests require [.env.external]. Copy [.env.external.example] and configure the requested provider or providers before running them.',
            );
        }

        Dotenv::createUnsafeMutable($path, '.env.external')->load();

        $prefix = $this->requiredExternalEnvironment('SPOOLRAIL_PREFIX');

        if (
            strlen((string) $prefix) > 24
            || preg_match('/\Aspoolrail-(?:[a-z0-9]+-)*external(?:-[a-z0-9]+)*\z/', (string) $prefix) !== 1
        ) {
            throw new RuntimeException(
                'External tests require SPOOLRAIL_PREFIX to begin with [spoolrail-], include an [external] segment, use only lowercase letters, digits, and single hyphens, and contain at most 24 characters.',
            );
        }

        $runId = bin2hex(random_bytes(4));

        $this->externalPrefix = $prefix;
        $this->externalResourceStem = "$prefix-";
        $this->externalTopic = "{$this->externalResourceStem}$runId-events";
        $this->externalSubscription = "consumer-$runId";

        config()->set('spoolrail.prefix', $prefix);
    }

    /**
     * @template TResult
     *
     * @param  Closure(): TResult  $callback
     * @return TResult
     */
    protected function runExternalOperationWithin(int $seconds, Closure $callback): mixed
    {
        $previousAsyncSignals = pcntl_async_signals();
        $previousAlarmHandler = pcntl_signal_get_handler(SIGALRM);
        $timeout = new RuntimeException("External broker operation exceeded [$seconds] seconds.");

        pcntl_async_signals(true);
        pcntl_signal(SIGALRM, static function () use ($timeout): never {
            throw $timeout;
        });
        pcntl_alarm($seconds);

        try {
            return $callback();
        } finally {
            pcntl_alarm(0);
            pcntl_signal(SIGALRM, $previousAlarmHandler);
            pcntl_async_signals($previousAsyncSignals);
        }
    }

    protected function requiredExternalEnvironment(string $name): string
    {
        $value = getenv($name);

        if (! is_string($value) || trim($value) === '') {
            throw new RuntimeException("External tests require [$name] in [.env.external].");
        }

        return $value;
    }
}
