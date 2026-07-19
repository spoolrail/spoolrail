<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Illuminate\Support\ServiceProvider;
use Spoolrail\Spoolrail\Console\ConsumeCommand;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

class SpoolrailServiceProvider extends ServiceProvider
{
    #[\Override]
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

        if ($this->app->runningInConsole()) {
            $this->commands(ConsumeCommand::class);
        }

        $routes = base_path('routes/subscriptions.php');

        if (is_file($routes)) {
            require $routes;
        }
    }
}
