<?php

declare(strict_types=1);

use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\RateLimiter;
use Illuminate\Cache\Repository as CacheRepository;
use Illuminate\Config\Repository as ConfigRepository;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;

test('throttles reports by supervision failure message', function (): void {
    // --- Arrange ---
    $limiter = new RateLimiter(new CacheRepository(new ArrayStore));
    $config = new ConfigRepository([
        'spoolrail' => ['consumer' => ['exception_cooldown' => 300]],
    ]);
    $first = ConsumerException::subscriptionFailed(
        'warehouse-orders',
        new RuntimeException('Broker unavailable.'),
    );
    $sameFailureCategory = ConsumerException::subscriptionFailed(
        'warehouse-orders',
        new LogicException('Credentials rejected.'),
    );
    $differentSubscription = ConsumerException::subscriptionFailed(
        'billing-orders',
        new RuntimeException('Broker unavailable.'),
    );

    // --- Act / Assert ---
    expect($first->report($limiter, $config))->toBeFalse();
    expect($sameFailureCategory->report($limiter, $config))->toBeTrue();
    expect($differentSubscription->report($limiter, $config))->toBeFalse();
});

test('reports when throttling is unavailable', function (): void {
    // --- Arrange ---
    $limiter = Mockery::mock(RateLimiter::class);
    $limiter->expects('attempt')->andThrow(new RuntimeException('Cache unavailable.'));
    $config = new ConfigRepository([
        'spoolrail' => ['consumer' => ['exception_cooldown' => 300]],
    ]);
    $failure = ConsumerException::subscriptionFailed(
        'warehouse-orders',
        new RuntimeException('Broker unavailable.'),
    );

    // --- Act / Assert ---
    expect($failure->report($limiter, $config))->toBeFalse();
});
