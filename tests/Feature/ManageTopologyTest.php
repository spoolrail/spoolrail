<?php

declare(strict_types=1);

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\Facades\Http;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\CurrentPrefixCannotBeRetiredException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqConfigurationException;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

test('preflights every referenced managed connection before applying any topology plan', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'primary');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections', [
        'primary' => ['driver' => 'primary-managed'],
        'secondary' => ['driver' => 'secondary-managed'],
        'local' => ['driver' => 'array'],
        'unreferenced' => ['driver' => 'must-not-resolve'],
    ]);

    $primaryPlan = Mockery::mock(TopologyPlan::class);
    $primaryPlan->shouldReceive('apply')->once();

    $secondaryPlan = Mockery::mock(TopologyPlan::class);
    $secondaryPlan->shouldReceive('apply')->once();

    $primaryDriver = Mockery::mock(Driver::class, ManagedTopology::class);
    $primaryDriver->shouldReceive('planSync')
        ->once()
        ->andReturnUsing(function (array $subscriptions, string $prefix) use ($primaryPlan): TopologyPlan {
            expect(array_map(
                static fn (Subscription $subscription): string => $subscription->name(),
                $subscriptions,
            ))->toBe(['primary-orders']);
            expect($prefix)->toBe('warehouse-production');

            return $primaryPlan;
        });

    $secondaryDriver = Mockery::mock(Driver::class, ManagedTopology::class);
    $secondaryDriver->shouldReceive('planSync')
        ->once()
        ->andReturnUsing(function (array $subscriptions, string $prefix) use ($secondaryPlan): TopologyPlan {
            expect(array_map(
                static fn (Subscription $subscription): string => $subscription->name(),
                $subscriptions,
            ))->toBe(['secondary-orders']);
            expect($prefix)->toBe('warehouse-production');

            return $secondaryPlan;
        });

    Spoolrail::extend(
        'primary-managed',
        fn (Application $app, array $config, string $name): Driver => $primaryDriver,
    );
    Spoolrail::extend(
        'secondary-managed',
        fn (Application $app, array $config, string $name): Driver => $secondaryDriver,
    );

    Spoolrail::subscribe('orders', 'primary-orders', NoopMessageHandler::class);
    Spoolrail::subscribe('orders', 'secondary-orders', NoopMessageHandler::class)
        ->onConnection('secondary');
    Spoolrail::subscribe('orders', 'local-orders', NoopMessageHandler::class)
        ->onConnection('local');

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:sync')
        ->expectsOutputToContain('Synchronized topology for connection [primary].')
        ->expectsOutputToContain('Synchronized topology for connection [secondary].')
        ->expectsOutputToContain('Connection [local] has no package-managed topology and was not changed.')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('continues preflighting later connections but applies no plan after an earlier failure', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'primary');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections', [
        'primary' => ['driver' => 'primary-managed'],
        'secondary' => ['driver' => 'secondary-managed'],
        'tertiary' => ['driver' => 'tertiary-managed'],
    ]);

    $primaryPlan = Mockery::mock(TopologyPlan::class);
    $primaryPlan->shouldNotReceive('apply');
    $preflightFailure = new RuntimeException('secondary topology is incompatible');
    $primaryDriver = Mockery::mock(Driver::class, ManagedTopology::class);
    $primaryDriver->shouldReceive('planSync')->once()->andReturn($primaryPlan);

    $secondaryDriver = Mockery::mock(Driver::class, ManagedTopology::class);
    $secondaryDriver->shouldReceive('planSync')->once()->andThrow($preflightFailure);

    $tertiaryPlan = Mockery::mock(TopologyPlan::class);
    $tertiaryPlan->shouldNotReceive('apply');
    $tertiaryDriver = Mockery::mock(Driver::class, ManagedTopology::class);
    $tertiaryDriver->shouldReceive('planSync')->once()->andReturn($tertiaryPlan);

    Spoolrail::extend(
        'primary-managed',
        fn (Application $app, array $config, string $name): Driver => $primaryDriver,
    );
    Spoolrail::extend(
        'secondary-managed',
        fn (Application $app, array $config, string $name): Driver => $secondaryDriver,
    );
    Spoolrail::extend(
        'tertiary-managed',
        fn (Application $app, array $config, string $name): Driver => $tertiaryDriver,
    );

    Spoolrail::subscribe('orders', 'primary-orders', NoopMessageHandler::class);
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
    expect($failure?->failures)->toBe(['secondary' => $preflightFailure]);
});

test('fails a built-in topology command before HTTP when management configuration is absent', function (): void {
    // --- Arrange ---
    Http::preventStrayRequests();

    config()->set('spoolrail.default', 'rabbit');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections.rabbit', [
        'driver' => 'rabbitmq',
        'url' => 'amqp://guest:guest@rabbit.internal/%2F',
    ]);

    Spoolrail::subscribe('orders', 'warehouse-orders', NoopMessageHandler::class);

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:sync')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(TopologyPreflightException::class);
    expect($failure?->failures['rabbit'] ?? null)->toBeInstanceOf(RabbitMqConfigurationException::class);
    Http::assertNothingSent();
});

test('uses the default connection, scopes its subscriptions, and deletes returned candidates', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'primary');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections', [
        'primary' => ['driver' => 'primary-managed'],
        'secondary' => ['driver' => 'secondary-managed'],
    ]);

    $driver = Mockery::mock(Driver::class, ManagedTopology::class);
    $driver->shouldReceive('undeclaredSubscriptions')
        ->once()
        ->andReturnUsing(function (array $subscriptions, string $prefix): array {
            expect(array_map(
                static fn (Subscription $subscription): string => $subscription->name(),
                $subscriptions,
            ))->toBe(['primary-orders']);
            expect($prefix)->toBe('warehouse-production');

            return [
                'warehouse-production-old-orders',
                'warehouse-production-old-returns',
            ];
        });
    $driver->shouldReceive('deleteSubscription')
        ->once()
        ->with('warehouse-production-old-orders');
    $driver->shouldReceive('deleteSubscription')
        ->once()
        ->with('warehouse-production-old-returns');

    Spoolrail::extend(
        'primary-managed',
        fn (Application $app, array $config, string $name): Driver => $driver,
    );

    Spoolrail::subscribe('orders', 'primary-orders', NoopMessageHandler::class);
    Spoolrail::subscribe('orders', 'secondary-orders', NoopMessageHandler::class)
        ->onConnection('secondary');

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:delete-undeclared-subscriptions')
        ->expectsOutputToContain('Inspecting connection [primary] with ownership prefix [warehouse-production].')
        ->expectsOutputToContain('Other potentially managed connections were not inspected: secondary.')
        ->expectsOutputToContain('Deleted subscription resource [warehouse-production-old-orders].')
        ->expectsOutputToContain('Deleted subscription resource [warehouse-production-old-returns].')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('deletes undeclared resources only from the explicitly selected connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'primary');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections', [
        'primary' => ['driver' => 'primary-managed'],
        'secondary' => ['driver' => 'secondary-managed'],
    ]);

    $primaryResolved = 0;
    $primaryDriver = Mockery::mock(Driver::class, ManagedTopology::class);
    $secondaryDriver = Mockery::mock(Driver::class, ManagedTopology::class);
    $secondaryDriver->shouldReceive('undeclaredSubscriptions')
        ->once()
        ->andReturnUsing(function (array $subscriptions, string $prefix): array {
            expect(array_map(
                static fn (Subscription $subscription): string => $subscription->name(),
                $subscriptions,
            ))->toBe(['secondary-orders']);
            expect($prefix)->toBe('warehouse-production');

            return ['warehouse-production-old-secondary-orders'];
        });
    $secondaryDriver->shouldReceive('deleteSubscription')
        ->once()
        ->with('warehouse-production-old-secondary-orders');

    Spoolrail::extend(
        'primary-managed',
        function (Application $app, array $config, string $name) use ($primaryDriver, &$primaryResolved): Driver {
            $primaryResolved++;

            return $primaryDriver;
        },
    );
    Spoolrail::extend(
        'secondary-managed',
        fn (Application $app, array $config, string $name): Driver => $secondaryDriver,
    );

    Spoolrail::subscribe('orders', 'primary-orders', NoopMessageHandler::class);
    Spoolrail::subscribe('orders', 'secondary-orders', NoopMessageHandler::class)
        ->onConnection('secondary');

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-undeclared-subscriptions --connection=secondary',
    )
        ->expectsOutputToContain(
            'Inspecting connection [secondary] with ownership prefix [warehouse-production].',
        )
        ->doesntExpectOutputToContain('Other potentially managed connections were not inspected')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($primaryResolved)->toBe(0);
});

test('treats every resource under an explicit retired prefix as undeclared', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'primary');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections.primary', ['driver' => 'primary-managed']);

    $driver = Mockery::mock(Driver::class, ManagedTopology::class);
    $driver->shouldReceive('undeclaredSubscriptions')
        ->once()
        ->andReturnUsing(function (array $subscriptions, string $prefix): array {
            expect($subscriptions)->toBe([]);
            expect($prefix)->toBe('warehouse-staging');

            return ['warehouse-staging-orders'];
        });
    $driver->shouldReceive('deleteSubscription')
        ->once()
        ->with('warehouse-staging-orders');

    Spoolrail::extend(
        'primary-managed',
        fn (Application $app, array $config, string $name): Driver => $driver,
    );
    Spoolrail::subscribe('orders', 'orders', NoopMessageHandler::class);

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-undeclared-subscriptions --retired-prefix=warehouse-staging',
    )->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('refuses to delete the current ownership prefix as retired', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'primary');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections.primary', ['driver' => 'primary-managed']);

    $driver = Mockery::mock(Driver::class, ManagedTopology::class);
    $driver->shouldNotReceive('undeclaredSubscriptions');

    Spoolrail::extend(
        'primary-managed',
        fn (Application $app, array $config, string $name): Driver => $driver,
    );

    // --- Act ---
    $failure = null;

    try {
        $this->artisan(
            'spoolrail:delete-undeclared-subscriptions --retired-prefix=warehouse-production',
        )->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(CurrentPrefixCannotBeRetiredException::class);
});

test('deletes a topic from the default or explicitly selected connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'primary');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections', [
        'primary' => ['driver' => 'primary-managed'],
        'secondary' => ['driver' => 'secondary-managed'],
    ]);

    $primaryDriver = Mockery::mock(Driver::class, ManagedTopology::class);
    $primaryDriver->shouldReceive('deleteTopic')->once()->with('orders');
    $secondaryDriver = Mockery::mock(Driver::class, ManagedTopology::class);
    $secondaryDriver->shouldReceive('deleteTopic')->once()->with('returns');

    Spoolrail::extend(
        'primary-managed',
        fn (Application $app, array $config, string $name): Driver => $primaryDriver,
    );
    Spoolrail::extend(
        'secondary-managed',
        fn (Application $app, array $config, string $name): Driver => $secondaryDriver,
    );

    // --- Act ---
    $defaultExitCode = $this->artisan('spoolrail:delete-topic orders')->run();
    $selectedExitCode = $this->artisan(
        'spoolrail:delete-topic returns --connection=secondary',
    )->run();

    // --- Assert ---
    expect($defaultExitCode)->toBe(0);
    expect($selectedExitCode)->toBe(0);
});

test('rejects explicit blank destructive options before resolving managed topology', function (string $command, string $message): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'primary');
    config()->set('spoolrail.prefix', 'warehouse-production');
    config()->set('spoolrail.connections.primary', ['driver' => 'primary-managed']);

    $connectionsResolved = 0;
    $driver = Mockery::mock(Driver::class, ManagedTopology::class);
    Spoolrail::extend(
        'primary-managed',
        function (Application $app, array $config, string $name) use ($driver, &$connectionsResolved): Driver {
            $connectionsResolved++;

            return $driver;
        },
    );

    // --- Act ---
    $exitCode = $this->artisan($command)
        ->expectsOutputToContain($message)
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(1);
    expect($connectionsResolved)->toBe(0);
})->with([
    'undeclared-subscription connection' => [
        'spoolrail:delete-undeclared-subscriptions --connection=',
        'The --connection option must name a Spoolrail connection.',
    ],
    'retired prefix' => [
        'spoolrail:delete-undeclared-subscriptions --retired-prefix=',
        'The --retired-prefix option must name a former ownership prefix.',
    ],
    'topic connection' => [
        'spoolrail:delete-topic orders --connection=',
        'The --connection option must name a Spoolrail connection.',
    ],
]);
