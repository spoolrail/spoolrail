<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use Illuminate\Cache\RateLimiter;
use Illuminate\Contracts\Config\Repository;
use RuntimeException;
use Throwable;

class ConsumerException extends RuntimeException implements SpoolrailException
{
    public function report(RateLimiter $limiter, Repository $config): bool
    {
        $cooldown = $config->get('spoolrail.consumer.exception_cooldown', 300);

        if (! is_int($cooldown) || $cooldown < 1) {
            return false;
        }

        $key = 'spoolrail:consumer:'.hash('sha256', $this->getMessage());

        try {
            $reportingAllowed = $limiter->attempt(
                $key,
                1,
                static fn (): bool => true,
                $cooldown,
            );
        } catch (Throwable) {
            return false;
        }

        return $reportingAllowed === false;
    }

    public static function connectionOptionWithSubscription(): self
    {
        return new self('The [--connection] option cannot be used when a subscription is selected.');
    }

    public static function arrayConnectionCannotBeSupervised(): self
    {
        return new self('Spoolrail cannot supervise every subscription on the in-process [array] connection. Select one subscription explicitly.');
    }

    public static function connectionHasNoSubscriptions(string $connectionName): self
    {
        return new self("Spoolrail connection [$connectionName] has no active subscriptions.");
    }

    public static function unsupportedTerminationCacheStore(string $store): self
    {
        return new self("Spoolrail consumer termination requires a cross-process default cache store; [$store] is not supported.");
    }

    public static function terminationSignalWasNotStored(): self
    {
        return new self('Spoolrail could not store the consumer termination signal.');
    }

    public static function subscriptionStoppedUnexpectedly(string $subscription): self
    {
        return new self("Spoolrail subscription [$subscription] stopped consuming unexpectedly.");
    }

    public static function subscriptionFailed(string $subscription, Throwable $previous): self
    {
        return new self(
            "Spoolrail subscription [$subscription] failed while consuming.",
            previous: $previous,
        );
    }

    public static function subscriptionProcessCouldNotStart(
        string $subscription,
        Throwable $previous,
    ): self {
        return new self(
            "Spoolrail could not start the consumer process for subscription [$subscription].",
            previous: $previous,
        );
    }

    public static function subscriptionProcessExitedUnexpectedly(
        string $subscription,
        string $reason,
    ): self {
        return new self("Spoolrail subscription [$subscription] process $reason.");
    }

    public static function terminationSignalCouldNotBeRead(Throwable $previous): self
    {
        return new self(
            'Spoolrail could not read the consumer termination signal.',
            previous: $previous,
        );
    }

    public static function unsupportedProcessRuntime(string $reason): self
    {
        return new self("Spoolrail consumer supervision is unavailable: $reason");
    }
}
