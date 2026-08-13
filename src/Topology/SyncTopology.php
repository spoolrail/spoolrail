<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use Illuminate\Support\Sleep;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Throwable;

class SyncTopology
{
    public function __construct(
        private SpoolrailManager $manager,
        private SubscriptionRegistry $subscriptions,
        private OwnershipPrefix $prefix,
    ) {}

    public function __invoke(): SyncResult
    {
        $subscriptionsByConnection = $this->subscriptionsByConnection();

        try {
            return $this->syncOnce($subscriptionsByConnection);
        } catch (TopologySyncRequiresRetryException) {
            Sleep::for(1)->second();
        }

        try {
            return $this->syncOnce($subscriptionsByConnection);
        } catch (TopologySyncRequiresRetryException $exception) {
            throw $exception->getPrevious() ?? $exception;
        }
    }

    /**
     * @param  array<string, list<Subscription>>  $subscriptionsByConnection
     */
    private function syncOnce(array $subscriptionsByConnection): SyncResult
    {
        $plansByConnection = [];
        $unmanagedConnectionNames = [];
        $failuresByConnection = [];
        $retryException = null;

        foreach ($subscriptionsByConnection as $connectionName => $subscriptions) {
            try {
                $topology = $this->manager->connection($connectionName)->topology();

                if (! $topology instanceof CanManageTopology) {
                    $unmanagedConnectionNames[] = $connectionName;

                    continue;
                }

                $plansByConnection[$connectionName] = $topology->planSync(
                    $subscriptions,
                    $this->prefix->current(),
                );
            } catch (TopologySyncRequiresRetryException $exception) {
                $retryException ??= $exception;
            } catch (Throwable $failure) {
                $failuresByConnection[$connectionName] = $failure;
            }
        }

        if ($failuresByConnection !== []) {
            throw new TopologyPreflightException($failuresByConnection);
        }

        if ($retryException instanceof TopologySyncRequiresRetryException) {
            throw $retryException;
        }

        foreach ($plansByConnection as $plan) {
            $plan->apply();
        }

        return new SyncResult(
            array_keys($plansByConnection),
            $unmanagedConnectionNames,
        );
    }

    /**
     * @return array<string, list<Subscription>>
     */
    private function subscriptionsByConnection(): array
    {
        $defaultConnectionName = $this->manager->defaultConnectionName();
        $subscriptionsByConnection = [];

        foreach ($this->subscriptions->all() as $subscription) {
            $subscriptionsByConnection[$subscription->connectionName($defaultConnectionName)][] = $subscription;
        }

        return $subscriptionsByConnection;
    }
}
