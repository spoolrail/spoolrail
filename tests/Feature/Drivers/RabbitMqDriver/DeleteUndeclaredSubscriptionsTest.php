<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\CurrentPrefixCannotBeRetiredException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithRabbitMq;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithRabbitMq::class);

test('warns when another managed connection is not inspected', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'rabbitmq');
    $this->addRabbitMqConnection('secondary');

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:delete-undeclared-subscriptions')
        ->expectsOutputToContain('Other potentially managed connections were not inspected: secondary.')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('deletes only undeclared subscriptions from an explicitly selected RabbitMQ connection', function (): void {
    // --- Arrange ---
    $prefix = app(OwnershipPrefix::class)->value();
    $declared = "$prefix-rabbit-orders";
    $undeclared = "$prefix-old-orders";

    Spoolrail::subscribe('orders', 'old-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'rabbit-orders', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');
    $this->artisan('spoolrail:sync')->run();
    $this->declareRabbitMqQueue($undeclared);

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-undeclared-subscriptions --connection=rabbitmq',
    )
        ->expectsOutputToContain(
            "Inspecting connection [rabbitmq] with ownership prefix [$prefix].",
        )
        ->expectsOutputToContain("Deleted subscription resource [$undeclared].")
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->rabbitMqQueueExists($declared))->toBeTrue();
    expect($this->rabbitMqQueueExists($undeclared))->toBeFalse();
});

test('deletes every RabbitMQ subscription under an explicit retired prefix', function (): void {
    // --- Arrange ---
    $retired = 'retired-application';
    $queue = "$retired-orders";
    $this->declareRabbitMqQueue($queue);

    Spoolrail::subscribe('orders', 'orders', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');

    // --- Act ---
    $exitCode = $this->artisan(
        "spoolrail:delete-undeclared-subscriptions --connection=rabbitmq --retired-prefix=$retired",
    )->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->rabbitMqQueueExists($queue))->toBeFalse();
});

test('refuses to delete the current ownership prefix as retired', function (): void {
    $prefix = app(OwnershipPrefix::class)->value();

    $action = fn () => $this->artisan(
        "spoolrail:delete-undeclared-subscriptions --connection=rabbitmq --retired-prefix=$prefix",
    )->run();

    expect($action)->toThrow(CurrentPrefixCannotBeRetiredException::class);
});
