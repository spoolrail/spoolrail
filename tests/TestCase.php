<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests;

use Orchestra\Testbench\TestCase as OrchestraTestCase;
use Spoolrail\Spoolrail\SpoolrailServiceProvider;

class TestCase extends OrchestraTestCase
{
    protected function getPackageProviders($app): array
    {
        return [
            SpoolrailServiceProvider::class,
        ];
    }
}
