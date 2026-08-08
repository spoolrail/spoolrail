<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Subscriptions\SubscriptionProcess;
use Spoolrail\Spoolrail\Subscriptions\SupervisedSubscription;

test('backs off consecutive failures on a fixed indefinitely capped schedule', function (): void {
    // --- Arrange ---
    $subscription = new SupervisedSubscription('warehouse-orders');
    $now = 0.0;

    foreach ([1, 5, 15, 30, 60, 60, 60] as $delay) {
        // --- Act ---
        $subscription->markAsFailed($now);

        // --- Assert ---
        expect($subscription->isReadyToStart($now + $delay - 0.1))->toBeFalse();
        expect($subscription->isReadyToStart($now + $delay))->toBeTrue();

        $now += 100;
    }
});

test('resets the failure streak only after a subscription remains active for sixty seconds', function (): void {
    // --- Arrange ---
    $unstable = new SupervisedSubscription('warehouse-orders');
    $stable = new SupervisedSubscription('billing-orders');
    $process = Mockery::mock(SubscriptionProcess::class);
    foreach ([$unstable, $stable] as $subscription) {
        $subscription->markAsFailed(0);
        $subscription->markAsFailed(10);
        $subscription->markAsStarted($process, 20);
    }

    // --- Act ---
    $unstableRecovered = $unstable->resetBackoffWhenStable(79.9);
    $stableRecovered = $stable->resetBackoffWhenStable(80);
    $stableRecoveredAgain = $stable->resetBackoffWhenStable(80.1);
    $unstable->markAsFailed(81);
    $stable->markAsFailed(81);

    // --- Assert ---
    expect($unstableRecovered)->toBeFalse();
    expect($stableRecovered)->toBeTrue();
    expect($stableRecoveredAgain)->toBeFalse();
    expect($unstable->isReadyToStart(95.9))->toBeFalse();
    expect($unstable->isReadyToStart(96))->toBeTrue();
    expect($stable->isReadyToStart(82))->toBeTrue();
});
