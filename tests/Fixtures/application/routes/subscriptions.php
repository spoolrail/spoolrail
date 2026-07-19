<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

Spoolrail::subscribe(
    'orders',
    'route-loaded-orders',
    NoopMessageHandler::class,
);
