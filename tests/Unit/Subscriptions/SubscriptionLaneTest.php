<?php

declare(strict_types=1);

use Illuminate\Contracts\Queue\Queue;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionLane;

test('restores the initial retry delay after sixty stable seconds', function (): void {
    // --- Arrange ---
    $lane = new SubscriptionLane(
        Mockery::mock(Subscription::class),
        Mockery::mock(Queue::class),
    );
    $lane->receiveStarted(0);
    $lane->receiveFailed(0);
    $lane->receiveStarted(1);
    $lane->received([]);

    // --- Act ---
    $recovered = $lane->resetBackoffWhenStable(61);
    $lane->receiveStarted(61);
    $lane->receiveFailed(61);

    // --- Assert ---
    expect($recovered)->toBeTrue();
    expect($lane->mayReceive(61.9))->toBeFalse();
    expect($lane->mayReceive(62))->toBeTrue();
});
