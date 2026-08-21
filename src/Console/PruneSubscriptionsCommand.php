<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use InvalidArgumentException;
use Spoolrail\Spoolrail\Exceptions\SubscriptionPruningException;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Topology\PlanSubscriptionPruning;

class PruneSubscriptionsCommand extends Command
{
    protected $signature = 'spoolrail:prune-subscriptions
                            {--connection= : Spoolrail connection to inspect}
                            {--retired-prefix= : Former ownership prefix to delete completely}
                            {--force : Delete without confirmation}';

    protected $description = 'Delete application-owned subscriptions no longer declared on one connection';

    public function handle(
        PlanSubscriptionPruning $planSubscriptionPruning,
        SpoolrailManager $manager,
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

        try {
            $pruningPlan = $planSubscriptionPruning($connectionName, $retiredPrefix);
        } catch (SubscriptionPruningException $exception) {
            $this->components->error($exception->getMessage());

            return self::FAILURE;
        }

        $this->components->info(
            "Inspecting connection [$connectionName] with ownership prefix [$pruningPlan->ownershipPrefix].",
        );
        $this->warnAboutUninspectedConnections($connectionOption, $connectionName, $manager);

        if ($pruningPlan->resourceNames === []) {
            $this->components->info('No undeclared subscription resources were found.');

            return self::SUCCESS;
        }

        if (! $this->confirmPruning($pruningPlan->resourceNames)) {
            return self::FAILURE;
        }

        $pruningPlan->apply();
        $this->reportDeletions($pruningPlan->resourceNames);

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
            $manager->connectionNamesThatMayManageTopology(),
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
     * @param  list<string>  $resourceNames
     */
    private function confirmPruning(array $resourceNames): bool
    {
        $count = count($resourceNames);
        $resourceLabel = $count === 1 ? 'resource' : 'resources';

        $this->components->warn(
            "Pruning will permanently delete $count subscription $resourceLabel and discard any messages waiting for delivery.",
        );
        $this->components->bulletList(array_map(
            static fn (string $resourceName): string => "[$resourceName]",
            $resourceNames,
        ));

        if ($this->option('force') === true) {
            return true;
        }

        if ($this->components->confirm('Delete the displayed subscription resources?', false)) {
            return true;
        }

        $this->components->warn('Subscription pruning cancelled. Use [--force] to skip confirmation.');

        return false;
    }

    /**
     * @param  list<string>  $resourceNames
     */
    private function reportDeletions(array $resourceNames): void
    {
        foreach ($resourceNames as $resourceName) {
            $this->components->info("Deleted subscription resource [$resourceName].");
        }
    }
}
