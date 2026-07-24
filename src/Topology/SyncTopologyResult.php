<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

class SyncTopologyResult
{
    /**
     * @param  list<string>  $managedConnections
     * @param  list<string>  $unmanagedConnections
     */
    public function __construct(
        public readonly array $managedConnections,
        public readonly array $unmanagedConnections,
    ) {}
}
