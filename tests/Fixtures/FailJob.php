<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Closure;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;

class FailJob
{
    public function handle(HandleMessageJob $job, Closure $next): void
    {
        $job->fail();
    }
}
