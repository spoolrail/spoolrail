<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Topology\DeleteUndeclaredSubscriptions;

class DeleteUndeclaredSubscriptionsCommand extends Command
{
    protected $signature = 'spoolrail:delete-undeclared-subscriptions
                            {--connection= : Spoolrail connection to inspect}
                            {--retired-prefix= : Former ownership prefix to delete completely}';

    protected $description = 'Delete application-owned receive resources no longer declared on one connection';

    public function handle(
        DeleteUndeclaredSubscriptions $deletion,
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

        $connection = $connectionOption ?? $manager->getDefaultConnection();
        $retiredPrefix = $retiredPrefixOption;
        $targetPrefix = $retiredPrefix === null
            ? $prefix->value()
            : $prefix->validate($retiredPrefix);

        $this->components->info("Inspecting connection [$connection] with ownership prefix [$targetPrefix].");

        if ($connectionOption === null) {
            $uninspected = array_values(array_diff(
                $manager->potentiallyManagedConnectionNames(),
                [$connection],
            ));

            if ($uninspected !== []) {
                $this->components->warn(
                    'Other potentially managed connections were not inspected: '.implode(', ', $uninspected).'.',
                );
            }
        }

        $deleted = $deletion->run($connection, $retiredPrefix);

        foreach ($deleted as $physicalName) {
            $this->components->info("Deleted subscription resource [$physicalName].");
        }

        if ($deleted === []) {
            $this->components->info('No undeclared subscription resources were found.');
        }

        return self::SUCCESS;
    }
}
