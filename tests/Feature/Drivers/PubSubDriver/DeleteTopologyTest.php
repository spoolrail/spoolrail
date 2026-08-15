<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\PubSubTopologyException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithPubSub;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithPubSub::class);

test('deletes retired-prefix subscriptions without touching the current prefix', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'current-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');
    $this->artisan('spoolrail:sync')->run();

    $retiredSubscription = new Subscription(
        'orders',
        'old-orders',
        RecordingMessageHandler::class,
        static function (): void {},
    );
    Spoolrail::connection('pubsub')->topology()?->planSync(
        [$retiredSubscription],
        'retired-application',
    )->apply();

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-undeclared-subscriptions --connection=pubsub --retired-prefix=retired-application',
    )
        ->expectsOutputToContain('Deleted subscription resource [retired-application-old-orders].')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->pubSubSubscription('current-orders')->exists())->toBeTrue();
    expect($this->pubSubSubscription('old-orders', 'retired-application')->exists())->toBeFalse();
});

test('refuses to delete a topic while it has subscriptions', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('pubsub');
    $this->artisan('spoolrail:sync')->run();

    // --- Act & Assert ---
    expect(fn () => $this->artisan(
        'spoolrail:delete-topic orders --connection=pubsub',
    )->run())->toThrow(
        PubSubTopologyException::class,
        'while it has subscriptions',
    );
    expect($this->pubSubTopic('orders')->exists())->toBeTrue();
    expect($this->pubSubSubscription('warehouse-orders')->exists())->toBeTrue();
});

test('deletes an empty topic explicitly', function (): void {
    // --- Arrange ---
    $this->pubsub->createTopic('orders');

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:delete-topic orders --connection=pubsub')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->pubSubTopic('orders')->exists())->toBeFalse();
});
