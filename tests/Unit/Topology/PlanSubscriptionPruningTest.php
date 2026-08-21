<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\SubscriptionPruningException;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Spoolrail\Spoolrail\Topology\PlanSubscriptionPruning;

test('requires an ownership prefix before discovering owned receive-side resources', function (): void {
    config()->set('spoolrail.prefix');

    $topology = Mockery::mock(CanManageTopology::class);
    $topology->shouldNotReceive('undeclaredSubscriptionResourceNames');

    $connection = Mockery::mock(Connection::class);
    $connection->expects('topology')->once()->andReturn($topology);

    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('connection')->once()->with('managed')->andReturn($connection);

    $plan = new PlanSubscriptionPruning(
        $manager,
        new SubscriptionRegistry,
        app(OwnershipPrefix::class),
    );

    expect(fn () => $plan('managed', null))
        ->toThrow(InvalidConfigException::class);
});

test('plans formerly valid prefix cleanup without deleting before application', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.prefix', 'current-app');
    $formerPrefix = 'GoOg'.str_repeat('a', 25);

    $topology = Mockery::mock(CanManageTopology::class);
    $topology->expects('undeclaredSubscriptionResourceNames')
        ->once()
        ->with([], $formerPrefix)
        ->andReturn(["$formerPrefix-orders"]);
    $topology->shouldNotReceive('deleteSubscription');

    $connection = Mockery::mock(Connection::class);
    $connection->expects('topology')->once()->andReturn($topology);

    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('connection')->once()->with('managed')->andReturn($connection);

    // --- Act ---
    $pruningPlan = (new PlanSubscriptionPruning(
        $manager,
        new SubscriptionRegistry,
        app(OwnershipPrefix::class),
    ))('managed', $formerPrefix);

    // --- Assert ---
    expect($pruningPlan->ownershipPrefix)->toBe($formerPrefix);
    expect($pruningPlan->resourceNames)->toBe(["$formerPrefix-orders"]);
});

test('refuses to discover undeclared resources when the selected connection has no declarations', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.prefix', 'current-app');

    $topology = Mockery::mock(CanManageTopology::class);
    $topology->shouldNotReceive('undeclaredSubscriptionResourceNames');
    $topology->shouldNotReceive('deleteSubscription');

    $connection = Mockery::mock(Connection::class);
    $connection->expects('topology')->once()->andReturn($topology);

    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('connection')->once()->with('managed')->andReturn($connection);
    $manager->expects('defaultConnectionName')->once()->andReturn('managed');

    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'other-orders', RecordingMessageHandler::class)
        ->onConnection('other');

    $plan = new PlanSubscriptionPruning(
        $manager,
        $subscriptions,
        app(OwnershipPrefix::class),
    );

    // --- Act & Assert ---
    expect(fn () => $plan('managed', null))->toThrow(
        SubscriptionPruningException::class,
        'Spoolrail connection [managed] has no declared subscriptions, so pruning its current ownership prefix was refused.',
    );
});
