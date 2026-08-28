<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Subscriptions\RecoveryBackoff;

test('backs off repeated failures on the fixed capped schedule', function (): void {
    // --- Arrange ---
    $recovery = new RecoveryBackoff;
    $now = 0.0;

    foreach ([1, 5, 15, 30, 60, 60, 60] as $delay) {
        // --- Act ---
        $recovery->recordFailure($now);

        // --- Assert ---
        expect($recovery->mayRetry($now + $delay - 0.1))->toBeFalse();
        expect($recovery->mayRetry($now + $delay))->toBeTrue();

        $now += 100;
    }
});

test('resets failure backoff only after sixty stable seconds', function (): void {
    // --- Arrange ---
    $unstable = new RecoveryBackoff;
    $stable = new RecoveryBackoff;

    foreach ([$unstable, $stable] as $recovery) {
        $recovery->recordFailure(0);
        $recovery->recordFailure(10);
        $recovery->recordStart(20);
    }

    // --- Act ---
    $unstableRecovered = $unstable->resetWhenStable(79.9);
    $stableRecovered = $stable->resetWhenStable(80);
    $stableRecoveredAgain = $stable->resetWhenStable(80.1);
    $unstable->recordFailure(81);
    $stable->recordFailure(81);

    // --- Assert ---
    expect($unstableRecovered)->toBeFalse();
    expect($stableRecovered)->toBeTrue();
    expect($stableRecoveredAgain)->toBeFalse();
    expect($unstable->mayRetry(95.9))->toBeFalse();
    expect($unstable->mayRetry(96))->toBeTrue();
    expect($stable->mayRetry(81.9))->toBeFalse();
    expect($stable->mayRetry(82))->toBeTrue();
});
