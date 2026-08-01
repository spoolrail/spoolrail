<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

readonly class SyncResult
{
    /**
     * @param  list<string>  $syncedConnectionNames
     * @param  list<string>  $unmanagedConnectionNames
     */
    public function __construct(
        public array $syncedConnectionNames,
        public array $unmanagedConnectionNames,
    ) {}
}
