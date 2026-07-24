<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

interface TopologyPlan
{
    public function apply(): void;
}
