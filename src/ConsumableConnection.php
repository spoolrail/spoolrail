<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Closure;
use Spoolrail\Spoolrail\Contracts\ConsumableDriver;
use Spoolrail\Spoolrail\Contracts\Delivery;
use Spoolrail\Spoolrail\Serialization\MessageSerializer;

class ConsumableConnection extends Connection
{
    public function __construct(
        private readonly ConsumableDriver $consumableDriver,
        MessageSerializer $serializer,
    ) {
        parent::__construct($consumableDriver, $serializer);
    }

    /**
     * @param  Closure(Delivery): void  $handle
     */
    public function consume(string $subscription, Closure $handle): void
    {
        $this->consumableDriver->consume($subscription, $handle);
    }
}
