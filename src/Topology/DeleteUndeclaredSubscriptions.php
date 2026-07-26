<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use Spoolrail\Spoolrail\Exceptions\CurrentPrefixCannotBeRetiredException;
use Spoolrail\Spoolrail\Exceptions\ManagedTopologyUnavailableException;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

readonly class DeleteUndeclaredSubscriptions
{
    public function __construct(
        private SpoolrailManager $manager,
        private SubscriptionRegistry $subscriptions,
        private OwnershipPrefix $prefix,
    ) {}

    /**
     * @return list<string>
     */
    public function run(string $connectionName, ?string $retiredPrefix): array
    {
        $currentPrefix = $this->prefix->value();
        $targetPrefix = $retiredPrefix === null
            ? $currentPrefix
            : $this->prefix->validate($retiredPrefix);

        if ($retiredPrefix !== null && $targetPrefix === $currentPrefix) {
            throw new CurrentPrefixCannotBeRetiredException($currentPrefix);
        }

        $topology = $this->manager->connection($connectionName)->managedTopology()
            ?? throw new ManagedTopologyUnavailableException($connectionName);

        $undeclared = $topology->undeclaredSubscriptions(
            $retiredPrefix === null ? $this->subscriptionsFor($connectionName) : [],
            $targetPrefix,
        );

        foreach ($undeclared as $physicalName) {
            $topology->deleteSubscription($physicalName);
        }

        return $undeclared;
    }

    /**
     * @return list<Subscription>
     */
    private function subscriptionsFor(string $connectionName): array
    {
        $default = $this->manager->getDefaultConnection();

        return array_values(array_filter(
            $this->subscriptions->all(),
            static fn (Subscription $subscription): bool => $subscription->connection($default) === $connectionName,
        ));
    }
}
