<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

use Closure;

interface ConsumableDriver extends Driver
{
    /**
     * @param  Closure(Delivery): void  $handle
     */
    public function consume(string $subscription, Closure $handle): void;
}
