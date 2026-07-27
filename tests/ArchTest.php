<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\SpoolrailException;

arch('no debugging')
    ->expect(['dd', 'dump', 'var_dump', 'die', 'ray'])
    ->not->toBeUsed();

arch('no direct env calls')
    ->expect('env')
    ->not->toBeUsed();

arch('keeps package exceptions within the package catch boundary')
    ->expect('Spoolrail\Spoolrail\Exceptions')
    ->classes()
    ->toImplement(SpoolrailException::class);

arch('keeps transport-neutral layers independent of concrete transports')
    ->expect([
        'Spoolrail\Spoolrail\Contracts',
        'Spoolrail\Spoolrail\Subscriptions',
        'Spoolrail\Spoolrail\Topology',
    ])
    ->not->toUse([
        'Spoolrail\Spoolrail\Drivers',
        'Spoolrail\Spoolrail\RabbitMq',
        'PhpAmqpLib',
    ]);
