<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Topology\DeleteUndeclaredSubscriptions;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

test('requires an ownership prefix before discovering owned receive-side resources', function (): void {
    config()->set('spoolrail.prefix');

    $topology = Mockery::mock(CanManageTopology::class);
    $topology->shouldNotReceive('undeclaredSubscriptionResourceNames');

    $connection = Mockery::mock(Connection::class);
    $connection->expects('topology')->once()->andReturn($topology);

    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('connection')->once()->with('managed')->andReturn($connection);

    $delete = new DeleteUndeclaredSubscriptions(
        $manager,
        new SubscriptionRegistry,
        app(OwnershipPrefix::class),
    );

    expect(fn (): array => $delete('managed', null))
        ->toThrow(InvalidConfigException::class);
});

test('accepts a formerly valid prefix for explicit cleanup', function (): void {
    config()->set('spoolrail.prefix', 'current-app');
    $formerPrefix = 'GoOg'.str_repeat('a', 25);

    $topology = Mockery::mock(CanManageTopology::class);
    $topology->expects('undeclaredSubscriptionResourceNames')
        ->once()
        ->with([], $formerPrefix)
        ->andReturn([]);

    $connection = Mockery::mock(Connection::class);
    $connection->expects('topology')->once()->andReturn($topology);

    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('connection')->once()->with('managed')->andReturn($connection);

    $deleted = (new DeleteUndeclaredSubscriptions(
        $manager,
        new SubscriptionRegistry,
        app(OwnershipPrefix::class),
    ))('managed', $formerPrefix);

    expect($deleted)->toBe([]);
});
