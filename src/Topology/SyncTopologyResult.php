<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

readonly class SyncTopologyResult
{
    /**
     * @param  list<string>  $managedConnectionNames
     * @param  list<string>  $unmanagedConnectionNames
     */
    public function __construct(
        public array $managedConnectionNames,
        public array $unmanagedConnectionNames,
    ) {}
}
