<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

interface CanWaitForConsumerIo
{
    /**
     * Give pending consumer operations one bounded opportunity to progress.
     */
    public function waitForConsumerIo(): void;
}
