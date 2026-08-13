<?php

declare(strict_types=1);

use Illuminate\Support\Sleep;
use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;
use Spoolrail\Spoolrail\Topology\SyncTopology;

afterEach(function (): void {
    Sleep::fake(false);
});

test('replans every connection and applies only remaining changes after an ambiguous result', function (): void {
    // --- Arrange ---
    Sleep::fake();
    $events = [];

    $initialFirstPlan = Mockery::mock(TopologyPlan::class);
    $initialFirstPlan->expects('apply')
        ->once()
        ->andReturnUsing(function () use (&$events): void {
            $events[] = 'apply first';
        });
    $noOpFirstPlan = Mockery::mock(TopologyPlan::class);
    $noOpFirstPlan->expects('apply')->once();
    $initialSecondPlan = Mockery::mock(TopologyPlan::class);
    $initialSecondPlan->expects('apply')
        ->once()
        ->andReturnUsing(function () use (&$events): never {
            $events[] = 'apply second';

            throw TopologySyncRequiresRetryException::afterFailure(new RuntimeException('Response lost.'));
        });
    $replannedSecondPlan = Mockery::mock(TopologyPlan::class);
    $replannedSecondPlan->expects('apply')
        ->once()
        ->andReturnUsing(function () use (&$events): void {
            $events[] = 'apply second';
        });

    $firstPlanningAttempt = 0;
    $firstTopology = Mockery::mock(CanManageTopology::class);
    $firstTopology->expects('planSync')
        ->twice()
        ->andReturnUsing(function () use (&$events, &$firstPlanningAttempt, $initialFirstPlan, $noOpFirstPlan): TopologyPlan {
            $events[] = 'plan first';
            $firstPlanningAttempt++;

            return $firstPlanningAttempt === 1
                ? $initialFirstPlan
                : $noOpFirstPlan;
        });
    $secondPlanningAttempt = 0;
    $secondTopology = Mockery::mock(CanManageTopology::class);
    $secondTopology->expects('planSync')
        ->twice()
        ->andReturnUsing(function () use (&$events, &$secondPlanningAttempt, $initialSecondPlan, $replannedSecondPlan): TopologyPlan {
            $events[] = 'plan second';
            $secondPlanningAttempt++;

            return $secondPlanningAttempt === 1
                ? $initialSecondPlan
                : $replannedSecondPlan;
        });

    $firstConnection = Mockery::mock(Connection::class);
    $firstConnection->expects('topology')->twice()->andReturn($firstTopology);
    $secondConnection = Mockery::mock(Connection::class);
    $secondConnection->expects('topology')->twice()->andReturn($secondTopology);

    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('defaultConnectionName')->once()->andReturn('first');
    $manager->expects('connection')->twice()->with('first')->andReturn($firstConnection);
    $manager->expects('connection')->twice()->with('second')->andReturn($secondConnection);

    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'first-orders', RecordingMessageHandler::class)
        ->onConnection('first');
    $subscriptions->subscribe('orders', 'second-orders', RecordingMessageHandler::class)
        ->onConnection('second');

    // --- Act ---
    $result = (new SyncTopology(
        $manager,
        $subscriptions,
        app(OwnershipPrefix::class),
    ))();

    // --- Assert ---
    expect($result->syncedConnectionNames)->toBe(['first', 'second']);
    expect($events)->toBe([
        'plan first',
        'plan second',
        'apply first',
        'apply second',
        'plan first',
        'plan second',
        'apply second',
    ]);
    Sleep::assertSleptTimes(1);
});

test('does not retry or apply plans when preflight finds a permanent failure', function (): void {
    // --- Arrange ---
    Sleep::fake();
    $plan = Mockery::mock(TopologyPlan::class);
    $plan->shouldNotReceive('apply');

    $plannedTopology = Mockery::mock(CanManageTopology::class);
    $plannedTopology->expects('planSync')->once()->andReturn($plan);
    $retryingTopology = Mockery::mock(CanManageTopology::class);
    $retryingTopology->expects('planSync')
        ->once()
        ->andThrow(TopologySyncRequiresRetryException::afterFailure(new RuntimeException('Rate limited.')));
    $permanentFailure = new RuntimeException('Permission denied.');
    $failingTopology = Mockery::mock(CanManageTopology::class);
    $failingTopology->expects('planSync')->once()->andThrow($permanentFailure);

    $plannedConnection = Mockery::mock(Connection::class);
    $plannedConnection->expects('topology')->once()->andReturn($plannedTopology);
    $retryingConnection = Mockery::mock(Connection::class);
    $retryingConnection->expects('topology')->once()->andReturn($retryingTopology);
    $failingConnection = Mockery::mock(Connection::class);
    $failingConnection->expects('topology')->once()->andReturn($failingTopology);

    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('defaultConnectionName')->once()->andReturn('planned');
    $manager->expects('connection')->once()->with('planned')->andReturn($plannedConnection);
    $manager->expects('connection')->once()->with('retrying')->andReturn($retryingConnection);
    $manager->expects('connection')->once()->with('failing')->andReturn($failingConnection);

    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'planned-orders', RecordingMessageHandler::class)
        ->onConnection('planned');
    $subscriptions->subscribe('orders', 'retrying-orders', RecordingMessageHandler::class)
        ->onConnection('retrying');
    $subscriptions->subscribe('orders', 'failing-orders', RecordingMessageHandler::class)
        ->onConnection('failing');

    // --- Act ---
    $caught = null;

    try {
        (new SyncTopology(
            $manager,
            $subscriptions,
            app(OwnershipPrefix::class),
        ))();
    } catch (TopologyPreflightException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->failuresByConnection['failing'] ?? null)->toBe($permanentFailure);
    Sleep::assertNeverSlept();
});

test('restarts preflight without applying partial plans after a retryable discovery failure', function (): void {
    // --- Arrange ---
    Sleep::fake();
    $events = [];
    $planningAttempts = 0;
    $firstPlan = Mockery::mock(TopologyPlan::class);
    $firstPlan->expects('apply')
        ->once()
        ->andReturnUsing(function () use (&$events): void {
            $events[] = 'apply first';
        });
    $secondPlan = Mockery::mock(TopologyPlan::class);
    $secondPlan->expects('apply')
        ->once()
        ->andReturnUsing(function () use (&$events): void {
            $events[] = 'apply second';
        });
    $firstTopology = Mockery::mock(CanManageTopology::class);
    $firstTopology->expects('planSync')
        ->twice()
        ->andReturnUsing(function () use (&$events, $firstPlan): TopologyPlan {
            $events[] = 'plan first';

            return $firstPlan;
        });
    $secondTopology = Mockery::mock(CanManageTopology::class);
    $secondTopology->expects('planSync')
        ->twice()
        ->andReturnUsing(function () use (&$events, &$planningAttempts, $secondPlan): TopologyPlan {
            $events[] = 'plan second';
            $planningAttempts++;

            if ($planningAttempts === 1) {
                throw TopologySyncRequiresRetryException::afterFailure(new RuntimeException('Rate limited.'));
            }

            return $secondPlan;
        });
    $firstConnection = Mockery::mock(Connection::class);
    $firstConnection->expects('topology')->twice()->andReturn($firstTopology);
    $secondConnection = Mockery::mock(Connection::class);
    $secondConnection->expects('topology')->twice()->andReturn($secondTopology);
    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('defaultConnectionName')->once()->andReturn('first');
    $manager->expects('connection')->twice()->with('first')->andReturn($firstConnection);
    $manager->expects('connection')->twice()->with('second')->andReturn($secondConnection);
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'first-orders', RecordingMessageHandler::class)
        ->onConnection('first');
    $subscriptions->subscribe('orders', 'second-orders', RecordingMessageHandler::class)
        ->onConnection('second');

    // --- Act ---
    $result = (new SyncTopology(
        $manager,
        $subscriptions,
        app(OwnershipPrefix::class),
    ))();

    // --- Assert ---
    expect($result->syncedConnectionNames)->toBe(['first', 'second']);
    expect($events)->toBe([
        'plan first',
        'plan second',
        'plan first',
        'plan second',
        'apply first',
        'apply second',
    ]);
    Sleep::assertSequence([
        Sleep::for(1000)->milliseconds(),
    ]);
});

test('retries topology synchronization once after a one-second wait', function (): void {
    // --- Arrange ---
    Sleep::fake();
    $failure = new RuntimeException('Broker response remained unavailable.');
    $plan = Mockery::mock(TopologyPlan::class);
    $plan->expects('apply')
        ->twice()
        ->andThrow(TopologySyncRequiresRetryException::afterFailure($failure));

    $topology = Mockery::mock(CanManageTopology::class);
    $topology->expects('planSync')->twice()->andReturn($plan);
    $connection = Mockery::mock(Connection::class);
    $connection->expects('topology')->twice()->andReturn($topology);
    $manager = Mockery::mock(SpoolrailManager::class);
    $manager->expects('defaultConnectionName')->once()->andReturn('events');
    $manager->expects('connection')->twice()->with('events')->andReturn($connection);
    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);

    // --- Act ---
    $caught = null;

    try {
        (new SyncTopology(
            $manager,
            $subscriptions,
            app(OwnershipPrefix::class),
        ))();
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
    Sleep::assertSequence([
        Sleep::for(1000)->milliseconds(),
    ]);
});

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
