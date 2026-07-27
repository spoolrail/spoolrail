<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithRabbitMq;

uses(InteractsWithRabbitMq::class);

test('deletes an unused topic from an explicitly selected RabbitMQ connection', function (): void {
    // --- Arrange ---
    $this->declareRabbitMqExchange('orders');

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-topic orders --connection=rabbitmq',
    )
        ->expectsOutputToContain('Deleted topic [orders] from connection [rabbitmq].')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->rabbitMqExchangeExists('orders'))->toBeFalse();
});
