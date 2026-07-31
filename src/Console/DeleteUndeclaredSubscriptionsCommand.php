<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use InvalidArgumentException;
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
        try {
            $connectionOption = $this->filledOption(
                'connection',
                'The --connection option must name a Spoolrail connection.',
            );
            $retiredPrefix = $this->filledOption(
                'retired-prefix',
                'The --retired-prefix option must name a former ownership prefix.',
            );
        } catch (InvalidArgumentException $exception) {
            $this->components->error($exception->getMessage());

            return self::FAILURE;
        }

        $connectionName = $connectionOption ?? $manager->defaultConnectionName();
        $targetPrefix = $retiredPrefix === null
            ? $prefix->current()
            : $prefix->validate($retiredPrefix);

        $this->components->info("Inspecting connection [$connectionName] with ownership prefix [$targetPrefix].");
        $this->warnAboutUninspectedConnections($connectionOption, $connectionName, $manager);

        $deletedResourceNames = $deleteUndeclaredSubscriptions($connectionName, $retiredPrefix);

        $this->reportDeletions($deletedResourceNames);

        return self::SUCCESS;
    }

    private function filledOption(string $name, string $errorMessage): ?string
    {
        /** @var ?string $value */
        $value = $this->option($name);

        if ($value === null) {
            return null;
        }

        if (trim($value) === '') {
            throw new InvalidArgumentException($errorMessage);
        }

        return $value;
    }

    private function warnAboutUninspectedConnections(
        ?string $connectionOption,
        string $connectionName,
        SpoolrailManager $manager,
    ): void {
        if ($connectionOption !== null) {
            return;
        }

        $uninspectedConnectionNames = array_values(array_diff(
            $manager->potentiallyManagedConnectionNames(),
            [$connectionName],
        ));

        if ($uninspectedConnectionNames === []) {
            return;
        }

        $this->components->warn(
            'Other potentially managed connections were not inspected: '.implode(', ', $uninspectedConnectionNames).'.',
        );
    }

    /**
     * @param  list<string>  $deletedResourceNames
     */
    private function reportDeletions(array $deletedResourceNames): void
    {
        if ($deletedResourceNames === []) {
            $this->components->info('No undeclared subscription resources were found.');

            return;
        }

        foreach ($deletedResourceNames as $resourceName) {
            $this->components->info("Deleted subscription resource [$resourceName].");
        }
    }
}
