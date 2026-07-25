<?php

declare(strict_types=1);

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\Facades\Http;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Exceptions\CurrentPrefixCannotBeRetiredException;
use Spoolrail\Spoolrail\Exceptions\InvalidPhysicalNameException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;

test('warns when another managed connection is not inspected', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'rabbitmq');
    config()->set('spoolrail.connections.secondary', ['driver' => 'rabbitmq']);

    $rabbitMq = Mockery::mock(Driver::class, ManagedTopology::class);
    $rabbitMq->shouldReceive('undeclaredSubscriptions')->once()->andReturn([]);
    Spoolrail::extend(
        'rabbitmq',
        fn (Application $app, array $config, string $name): Driver => $rabbitMq,
    );

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:delete-undeclared-subscriptions')
        ->expectsOutputToContain('Other potentially managed connections were not inspected: secondary.')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('deletes undeclared subscriptions from an explicitly selected connection', function (): void {
    // --- Arrange ---
    $prefix = app(OwnershipPrefix::class)->value();
    $undeclaredSubscription = "$prefix-old-orders";

    $rabbitMq = Mockery::mock(Driver::class, ManagedTopology::class);
    $rabbitMq->shouldReceive('undeclaredSubscriptions')
        ->once()
        ->andReturnUsing(function (array $subscriptions, string $ownershipPrefix) use ($prefix, $undeclaredSubscription): array {
            expect(array_map(
                static fn (Subscription $subscription): string => $subscription->name(),
                $subscriptions,
            ))->toBe(['rabbit-orders']);
            expect($ownershipPrefix)->toBe($prefix);

            return [$undeclaredSubscription];
        });
    $rabbitMq->shouldReceive('deleteSubscription')
        ->once()
        ->with($undeclaredSubscription);
    Spoolrail::extend(
        'rabbitmq',
        fn (Application $app, array $config, string $name): Driver => $rabbitMq,
    );

    Spoolrail::subscribe('orders', 'local-orders', NoopMessageHandler::class);
    Spoolrail::subscribe('orders', 'rabbit-orders', NoopMessageHandler::class)
        ->onConnection('rabbitmq');

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-undeclared-subscriptions --connection=rabbitmq',
    )
        ->expectsOutputToContain(
            "Inspecting connection [rabbitmq] with ownership prefix [$prefix].",
        )
        ->expectsOutputToContain("Deleted subscription resource [$undeclaredSubscription].")
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('treats every resource under an explicit retired prefix as undeclared', function (): void {
    // --- Arrange ---
    $rabbitMq = Mockery::mock(Driver::class, ManagedTopology::class);
    $rabbitMq->shouldReceive('undeclaredSubscriptions')
        ->once()
        ->with([], 'retired-application')
        ->andReturn(['retired-application-orders']);
    $rabbitMq->shouldReceive('deleteSubscription')
        ->once()
        ->with('retired-application-orders');
    Spoolrail::extend(
        'rabbitmq',
        fn (Application $app, array $config, string $name): Driver => $rabbitMq,
    );

    Spoolrail::subscribe('orders', 'orders', NoopMessageHandler::class)
        ->onConnection('rabbitmq');

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-undeclared-subscriptions --connection=rabbitmq --retired-prefix=retired-application',
    )->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('refuses to delete the current ownership prefix as retired', function (): void {
    $prefix = app(OwnershipPrefix::class)->value();

    $action = fn () => $this->artisan(
        "spoolrail:delete-undeclared-subscriptions --connection=rabbitmq --retired-prefix=$prefix",
    )->run();

    expect($action)->toThrow(CurrentPrefixCannotBeRetiredException::class);
});

test('rejects over-limit physical names before topology discovery', function (): void {
    Http::preventStrayRequests();
    config()->set('spoolrail.prefix', 'a'.str_repeat('b', 249));

    Spoolrail::subscribe('orders', 'orders', NoopMessageHandler::class)
        ->onConnection('rabbitmq');

    expect(fn () => $this->artisan(
        'spoolrail:delete-undeclared-subscriptions --connection=rabbitmq',
    )->run())->toThrow(InvalidPhysicalNameException::class);
    Http::assertNothingSent();
});
