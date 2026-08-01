<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\Topology\SyncTopology;

class SyncCommand extends Command
{
    protected $signature = 'spoolrail:sync';

    protected $description = 'Validate and create declared Spoolrail topology';

    public function handle(SyncTopology $syncTopology): int
    {
        $result = $syncTopology();

        foreach ($result->syncedConnectionNames as $connectionName) {
            $this->components->info("Synchronized topology for connection [$connectionName].");
        }

        foreach ($result->unmanagedConnectionNames as $connectionName) {
            $this->components->warn("Connection [$connectionName] has no package-managed topology and was not changed.");
        }

        return self::SUCCESS;
    }
}
