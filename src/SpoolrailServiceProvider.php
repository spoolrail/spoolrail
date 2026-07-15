<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Illuminate\Support\ServiceProvider;

class SpoolrailServiceProvider extends ServiceProvider
{
    #[\Override]
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/spoolrail.php', 'spoolrail');
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/spoolrail.php' => config_path('spoolrail.php'),
        ], 'spoolrail-config');
    }
}
