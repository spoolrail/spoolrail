<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Topology\DeleteUndeclaredSubscriptions;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

class DeleteUndeclaredSubscriptionsCommand extends Command
{
    protected $signature = 'spoolrail:delete-undeclared-subscriptions
                            {--connection= : Spoolrail connection to inspect}
                            {--retired-prefix= : Former ownership prefix to delete completely}';

    protected $description = 'Delete application-owned receive resources no longer declared on one connection';

    public function handle(
        DeleteUndeclaredSubscriptions $deleteUndeclaredSubscriptions,
        SpoolrailManager $manager,
        OwnershipPrefix $prefix,
    ): int {
        $connectionOption = $this->option('connection');
        $retiredPrefixOption = $this->option('retired-prefix');

        if ($connectionOption !== null && trim($connectionOption) === '') {
            $this->components->error('The --connection option must name a Spoolrail connection.');

            return self::FAILURE;
        }

        if ($retiredPrefixOption !== null && trim($retiredPrefixOption) === '') {
            $this->components->error('The --retired-prefix option must name a former ownership prefix.');

            return self::FAILURE;
        }

        $connectionName = $connectionOption ?? $manager->defaultConnectionName();
        $retiredPrefix = $retiredPrefixOption;
        $targetPrefix = $retiredPrefix === null
            ? $prefix->current()
            : $prefix->validate($retiredPrefix);

        $this->components->info("Inspecting connection [$connectionName] with ownership prefix [$targetPrefix].");

        if ($connectionOption === null) {
            $uninspectedConnectionNames = array_values(array_diff(
                $manager->potentiallyManagedConnectionNames(),
                [$connectionName],
            ));

            if ($uninspectedConnectionNames !== []) {
                $this->components->warn(
                    'Other potentially managed connections were not inspected: '.implode(', ', $uninspectedConnectionNames).'.',
                );
            }
        }

        $deletedResourceNames = $deleteUndeclaredSubscriptions($connectionName, $retiredPrefix);

        foreach ($deletedResourceNames as $resourceName) {
            $this->components->info("Deleted subscription resource [$resourceName].");
        }

        if ($deletedResourceNames === []) {
            $this->components->info('No undeclared subscription resources were found.');
        }

        return self::SUCCESS;
    }
}
