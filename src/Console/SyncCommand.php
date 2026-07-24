<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\Topology\SyncTopology;

class SyncCommand extends Command
{
    protected $signature = 'spoolrail:sync';

    protected $description = 'Validate and create declared Spoolrail topology';

    public function handle(SyncTopology $topology): int
    {
        $result = $topology->run();

        foreach ($result->managedConnections as $connection) {
            $this->components->info("Synchronized topology for connection [$connection].");
        }

        foreach ($result->unmanagedConnections as $connection) {
            $this->components->warn("Connection [$connection] has no package-managed topology and was not changed.");
        }

        return self::SUCCESS;
    }
}
