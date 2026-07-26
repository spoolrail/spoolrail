<?php

declare(strict_types=1);

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\Facades\Http;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\InvalidPhysicalNameException;
use Spoolrail\Spoolrail\Exceptions\InvalidRabbitMqTopicNameException;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

test('synchronizes every referenced managed connection after preflight', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.secondary', ['driver' => 'rabbitmq']);
    $prefix = app(OwnershipPrefix::class)->value();

    $rabbitMqPlan = Mockery::mock(TopologyPlan::class);
    $rabbitMqPlan->shouldReceive('apply')->once();
    $secondaryPlan = Mockery::mock(TopologyPlan::class);
    $secondaryPlan->shouldReceive('apply')->once();

    $rabbitMq = Mockery::mock(Driver::class, ManagedTopology::class);
    $rabbitMq->shouldReceive('planSync')
        ->once()
        ->andReturnUsing(function (array $subscriptions, string $ownershipPrefix) use ($rabbitMqPlan, $prefix): TopologyPlan {
            expect(array_map(
                static fn (Subscription $subscription): string => $subscription->name(),
                $subscriptions,
            ))->toBe(['rabbit-orders']);
            expect($ownershipPrefix)->toBe($prefix);

            return $rabbitMqPlan;
        });

    $secondary = Mockery::mock(Driver::class, ManagedTopology::class);
    $secondary->shouldReceive('planSync')
        ->once()
        ->andReturnUsing(function (array $subscriptions, string $ownershipPrefix) use ($secondaryPlan, $prefix): TopologyPlan {
            expect(array_map(
                static fn (Subscription $subscription): string => $subscription->name(),
                $subscriptions,
            ))->toBe(['secondary-orders']);
            expect($ownershipPrefix)->toBe($prefix);

            return $secondaryPlan;
        });

    $drivers = [
        'rabbitmq' => $rabbitMq,
        'secondary' => $secondary,
    ];
    Spoolrail::extend(
        'rabbitmq',
        fn (Application $app, array $config, string $name): Driver => $drivers[$name],
    );

    Spoolrail::subscribe('orders', 'rabbit-orders', NoopMessageHandler::class)
        ->onConnection('rabbitmq');
    Spoolrail::subscribe('orders', 'secondary-orders', NoopMessageHandler::class)
        ->onConnection('secondary');
    Spoolrail::subscribe('orders', 'local-orders', NoopMessageHandler::class);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:sync')
        ->expectsOutputToContain('Synchronized topology for connection [rabbitmq].')
        ->expectsOutputToContain('Synchronized topology for connection [secondary].')
        ->expectsOutputToContain('Connection [array] has no package-managed topology and was not changed.')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('reports every preflight failure before applying a valid plan', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.secondary', ['driver' => 'rabbitmq']);
    config()->set('spoolrail.connections.tertiary', ['driver' => 'rabbitmq']);

    $rabbitMqPlan = Mockery::mock(TopologyPlan::class);
    $rabbitMqPlan->shouldNotReceive('apply');
    $secondaryFailure = new RuntimeException('secondary topology is incompatible');
    $tertiaryFailure = new RuntimeException('tertiary topology is incompatible');

    $rabbitMq = Mockery::mock(Driver::class, ManagedTopology::class);
    $rabbitMq->shouldReceive('planSync')->once()->andReturn($rabbitMqPlan);
    $secondary = Mockery::mock(Driver::class, ManagedTopology::class);
    $secondary->shouldReceive('planSync')->once()->andThrow($secondaryFailure);
    $tertiary = Mockery::mock(Driver::class, ManagedTopology::class);
    $tertiary->shouldReceive('planSync')->once()->andThrow($tertiaryFailure);

    $drivers = [
        'rabbitmq' => $rabbitMq,
        'secondary' => $secondary,
        'tertiary' => $tertiary,
    ];
    Spoolrail::extend(
        'rabbitmq',
        fn (Application $app, array $config, string $name): Driver => $drivers[$name],
    );

    Spoolrail::subscribe('orders', 'rabbit-orders', NoopMessageHandler::class)
        ->onConnection('rabbitmq');
    Spoolrail::subscribe('orders', 'secondary-orders', NoopMessageHandler::class)
        ->onConnection('secondary');
    Spoolrail::subscribe('orders', 'tertiary-orders', NoopMessageHandler::class)
        ->onConnection('tertiary');

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:sync')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(TopologyPreflightException::class);
    expect($failure?->failures)->toBe([
        'secondary' => $secondaryFailure,
        'tertiary' => $tertiaryFailure,
    ]);
});

test('rejects over-limit physical names before topology discovery', function (string $topic, string $prefix, string $exception): void {
    Http::preventStrayRequests();
    config()->set('spoolrail.prefix', $prefix);

    Spoolrail::subscribe($topic, 'orders', NoopMessageHandler::class)
        ->onConnection('rabbitmq');

    $failure = null;

    try {
        $this->artisan('spoolrail:sync')->run();
    } catch (TopologyPreflightException $caught) {
        $failure = $caught;
    }

    expect($failure?->failures['rabbitmq'] ?? null)->toBeInstanceOf($exception);
    Http::assertNothingSent();
})->with([
    'topic' => [
        str_repeat('a', 256),
        'warehouse-production',
        InvalidRabbitMqTopicNameException::class,
    ],
    'queue with ownership prefix' => [
        'orders',
        'a'.str_repeat('b', 249),
        InvalidPhysicalNameException::class,
    ],
]);
