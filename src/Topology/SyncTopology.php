<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Throwable;

readonly class SyncTopology
{
    public function __construct(
        private SpoolrailManager $manager,
        private SubscriptionRegistry $subscriptions,
        private OwnershipPrefix $prefix,
    ) {}

    public function run(): SyncTopologyResult
    {
        $grouped = $this->groupSubscriptions();
        $plans = [];
        $unmanaged = [];
        $failures = [];

        foreach ($grouped as $connectionName => $subscriptions) {
            try {
                $topology = $this->manager->connection($connectionName)->managedTopology();

                if (! $topology instanceof ManagedTopology) {
                    $unmanaged[] = $connectionName;

                    continue;
                }

                $plans[$connectionName] = $topology->planSync($subscriptions, $this->prefix->value());
            } catch (Throwable $failure) {
                $failures[$connectionName] = $failure;
            }
        }

        if ($failures !== []) {
            throw new TopologyPreflightException($failures);
        }

        foreach ($plans as $plan) {
            $plan->apply();
        }

        return new SyncTopologyResult(array_keys($plans), $unmanaged);
    }

    /**
     * @return array<string, list<Subscription>>
     */
    private function groupSubscriptions(): array
    {
        $default = $this->manager->getDefaultConnection();
        $grouped = [];

        foreach ($this->subscriptions->all() as $subscription) {
            $grouped[$subscription->connection($default)][] = $subscription;
        }

        return $grouped;
    }
}
