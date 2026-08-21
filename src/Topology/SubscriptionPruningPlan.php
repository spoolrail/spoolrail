<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use Spoolrail\Spoolrail\Contracts\CanManageTopology;

readonly class SubscriptionPruningPlan
{
    /**
     * @param  list<string>  $resourceNames
     */
    public function __construct(
        private CanManageTopology $topology,
        public string $ownershipPrefix,
        public array $resourceNames,
    ) {}

    public function apply(): void
    {
        foreach ($this->resourceNames as $resourceName) {
            $this->topology->deleteSubscription($resourceName);
        }
    }
}
