<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Illuminate\Queue\Attributes\Timeout;

#[Timeout(75)]
trait HandlerQueuePolicyTimeout
{
    // Queue policy is declared on the trait.
}
