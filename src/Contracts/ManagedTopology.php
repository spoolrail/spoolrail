<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

use Spoolrail\Spoolrail\Subscriptions\Subscription;

interface ManagedTopology
{
    /**
     * @param  list<Subscription>  $subscriptions
     */
    public function planSync(array $subscriptions, string $ownershipPrefix): TopologyPlan;

    /**
     * @param  list<Subscription>  $subscriptions
     * @return list<string>
     */
    public function undeclaredSubscriptions(
        array $subscriptions,
        string $ownershipPrefix,
    ): array;

    public function deleteSubscription(string $physicalName): void;

    public function deleteTopic(string $topic): void;
}
