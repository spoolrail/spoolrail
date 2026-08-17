<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Illuminate\Support\ServiceProvider;
use Override;
use Spoolrail\Spoolrail\Console\ConsumeCommand;
use Spoolrail\Spoolrail\Console\DeleteTopicCommand;
use Spoolrail\Spoolrail\Console\DeleteUndeclaredSubscriptionsCommand;
use Spoolrail\Spoolrail\Console\PublishCommand;
use Spoolrail\Spoolrail\Console\PublishLanesCommand;
use Spoolrail\Spoolrail\Console\SpoolrailCommand;
use Spoolrail\Spoolrail\Console\SyncCommand;
use Spoolrail\Spoolrail\Console\TerminateCommand;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

class SpoolrailServiceProvider extends ServiceProvider
{
    #[Override]
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/spoolrail.php', 'spoolrail');

        $this->app->singleton(SpoolrailManager::class);
        $this->app->alias(SpoolrailManager::class, 'spoolrail');

        $this->app->singleton(SubscriptionRegistry::class);
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/spoolrail.php' => config_path('spoolrail.php'),
        ], 'spoolrail-config');

        $this->publishesMigrations([
            __DIR__.'/../database/migrations/0001_01_01_000000_create_outbox_publications_table.php' => database_path('migrations/0001_01_01_000000_create_outbox_publications_table.php'),
        ], 'spoolrail-migrations');

        if ($this->app->runningInConsole()) {
            $this->commands([
                SpoolrailCommand::class,
                ConsumeCommand::class,
                DeleteTopicCommand::class,
                DeleteUndeclaredSubscriptionsCommand::class,
                PublishCommand::class,
                PublishLanesCommand::class,
                SyncCommand::class,
                TerminateCommand::class,
            ]);
        }

        $routes = base_path('routes/subscriptions.php');

        if (is_file($routes)) {
            require $routes;
        }
    }
}
