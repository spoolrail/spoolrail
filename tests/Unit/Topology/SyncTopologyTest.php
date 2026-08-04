<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Spoolrail\Spoolrail\Topology\SyncTopology;

test('leaves topology-free connections usable without an ownership prefix', function (): void {
    config()->set('spoolrail.prefix');

    $connection = Mockery::mock(Connection::class);
    $connection->expects('topology')->once()->andReturnNull();

    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('defaultConnectionName')->once()->andReturn('array');
    $manager->expects('connection')->once()->with('array')->andReturn($connection);

    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);

    $result = (new SyncTopology(
        $manager,
        $subscriptions,
        app(OwnershipPrefix::class),
    ))();

    expect($result->syncedConnectionNames)->toBe([])
        ->and($result->unmanagedConnectionNames)->toBe(['array']);
});

test('requires an ownership prefix before managed topology preflight', function (): void {
    config()->set('spoolrail.prefix');

    $topology = Mockery::mock(CanManageTopology::class);
    $topology->shouldNotReceive('planSync');

    $connection = Mockery::mock(Connection::class);
    $connection->expects('topology')->once()->andReturn($topology);

    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('defaultConnectionName')->once()->andReturn('managed');
    $manager->expects('connection')->once()->with('managed')->andReturn($connection);

    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);

    $failure = null;

    try {
        (new SyncTopology(
            $manager,
            $subscriptions,
            app(OwnershipPrefix::class),
        ))();
    } catch (TopologyPreflightException $exception) {
        $failure = $exception;
    }

    expect($failure)->toBeInstanceOf(TopologyPreflightException::class)
        ->and($failure?->failuresByConnection['managed'] ?? null)
        ->toBeInstanceOf(InvalidConfigException::class);
});
