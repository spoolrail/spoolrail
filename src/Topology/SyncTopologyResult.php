<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

readonly class SyncTopologyResult
{
    /**
     * @param  list<string>  $managedConnections
     * @param  list<string>  $unmanagedConnections
     */
    public function __construct(
        public array $managedConnections,
        public array $unmanagedConnections,
    ) {}
}
