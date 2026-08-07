<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
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
        $plansByConnection = [];
        $unmanagedConnectionNames = [];
        $failuresByConnection = [];

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
            } catch (Throwable $failure) {
                $failuresByConnection[$connectionName] = $failure;
            }
        }

        if ($failuresByConnection !== []) {
            throw new TopologyPreflightException($failuresByConnection);
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
