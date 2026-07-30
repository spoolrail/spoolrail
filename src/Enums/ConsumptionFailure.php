<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Enums;

enum ConsumptionFailure
{
    case ConsumerStopped;
    case SettlementFailed;
}
