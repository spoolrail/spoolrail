<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\CurrentOwnershipPrefixCannotBeRetiredException;
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
    $prefix = app(OwnershipPrefix::class)->current();
    $declaredQueueName = "$prefix-rabbit-orders";
    $undeclaredQueueName = "$prefix-old-orders";

    Spoolrail::subscribe('orders', 'old-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'rabbit-orders', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');
    $this->artisan('spoolrail:sync')->run();
    $this->declareRabbitMqQueue($undeclaredQueueName);

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-undeclared-subscriptions --connection=rabbitmq',
    )
        ->expectsOutputToContain(
            "Inspecting connection [rabbitmq] with ownership prefix [$prefix].",
        )
        ->expectsOutputToContain("Deleted subscription resource [$undeclaredQueueName].")
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->rabbitMqQueueExists($declaredQueueName))->toBeTrue();
    expect($this->rabbitMqQueueExists($undeclaredQueueName))->toBeFalse();
});

test('deletes every RabbitMQ subscription under an explicit retired prefix', function (): void {
    // --- Arrange ---
    $retiredPrefix = 'retired-application';
    $queueName = "$retiredPrefix-orders";
    $this->declareRabbitMqQueue($queueName);

    Spoolrail::subscribe('orders', 'orders', RecordingMessageHandler::class)
        ->onConnection('rabbitmq');

    // --- Act ---
    $exitCode = $this->artisan(
        "spoolrail:delete-undeclared-subscriptions --connection=rabbitmq --retired-prefix=$retiredPrefix",
    )->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->rabbitMqQueueExists($queueName))->toBeFalse();
});

test('refuses to delete the current ownership prefix as retired', function (): void {
    $prefix = app(OwnershipPrefix::class)->current();

    $action = fn () => $this->artisan(
        "spoolrail:delete-undeclared-subscriptions --connection=rabbitmq --retired-prefix=$prefix",
    )->run();

    expect($action)->toThrow(CurrentOwnershipPrefixCannotBeRetiredException::class);
});
