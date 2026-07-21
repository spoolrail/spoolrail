<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Closure;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;

class QueuePolicyReleasingMiddleware
{
    public function handle(HandleMessageJob $job, Closure $next): void
    {
        $job->release(60);
    }
}
