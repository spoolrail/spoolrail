<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Illuminate\Filesystem\Filesystem;
use Symfony\Component\Finder\SplFileInfo;

class InstallCommand extends Command
{
    protected $signature = 'spoolrail:install
                            {--force : Overwrite existing Spoolrail files}
                            {--migrations : Publish the transactional outbox migration}';

    protected $description = 'Install the Spoolrail resources';

    public function handle(Filesystem $files): int
    {
        $this->components->info('Installing Spoolrail resources.');

        if ($this->option('migrations') && $this->publishMigrations($files) !== self::SUCCESS) {
            return self::FAILURE;
        }

        $this->publishFiles();

        $this->components->info('Spoolrail scaffolding installed successfully.');

        return self::SUCCESS;
    }

    private function publishFiles(): void
    {
        $options = ['--tag' => 'spoolrail-install'];

        if ($this->option('force')) {
            $options['--force'] = true;
        }

        $this->call('vendor:publish', $options);
    }

    private function publishMigrations(Filesystem $files): int
    {
        $directory = $this->laravel->databasePath('migrations');
        $files->ensureDirectoryExists($directory);

        $migrations = collect($files->files($directory))
            ->filter(static fn (SplFileInfo $file): bool => str_ends_with(
                $file->getFilename(),
                '_create_outbox_publications_table.php',
            ))
            ->map(static fn (SplFileInfo $file): string => $file->getPathname())
            ->values()
            ->all();

        if ($migrations === []) {
            return $this->call('vendor:publish', ['--tag' => 'spoolrail-migrations']);
        }

        if (count($migrations) !== 1) {
            $this->components->error(
                'Multiple outbox migrations already exist: ['.implode('], [', $migrations).'].',
            );

            return self::FAILURE;
        }

        if (! $this->option('force')) {
            $this->components->twoColumnDetail(
                'Outbox migration already exists',
                '<fg=yellow;options=bold>SKIPPED</>',
            );

            return self::SUCCESS;
        }

        return $this->replaceMigration($files, $migrations[0]);
    }

    private function replaceMigration(Filesystem $files, string $migration): int
    {
        if (! $files->copy(
            __DIR__.'/../../database/migrations/0001_01_01_000000_create_outbox_publications_table.php',
            $migration,
        )) {
            $this->components->error('Unable to publish the outbox migration.');

            return self::FAILURE;
        }

        $this->components->info('Published outbox migration.');

        return self::SUCCESS;
    }
}
