<?php

declare(strict_types=1);

use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\Facades\Http;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Exceptions\InvalidRabbitMqTopicNameException;
use Spoolrail\Spoolrail\Facades\Spoolrail;

test('deletes a topic from an explicitly selected connection', function (): void {
    // --- Arrange ---
    $rabbitMq = Mockery::mock(Driver::class, ManagedTopology::class);
    $rabbitMq->shouldReceive('deleteTopic')->once()->with('orders');
    Spoolrail::extend(
        'rabbitmq',
        fn (Application $app, array $config, string $name): Driver => $rabbitMq,
    );

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-topic orders --connection=rabbitmq',
    )->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('rejects an over-limit topic before topology discovery', function (): void {
    Http::preventStrayRequests();

    expect(fn () => $this->artisan(
        'spoolrail:delete-topic '.str_repeat('a', 256).' --connection=rabbitmq',
    )->run())->toThrow(InvalidRabbitMqTopicNameException::class);
    Http::assertNothingSent();
});
