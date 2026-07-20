<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Closure;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;

class RecordingArrayDriver extends ArrayDriver
{
    public int $consumeCalls = 0;

    /**
     * @param  Closure(string): void  $handoff
     */
    #[\Override]
    public function consume(string $subscription, Closure $handoff): void
    {
        $this->consumeCalls++;

        parent::consume($subscription, $handoff);
    }
}
