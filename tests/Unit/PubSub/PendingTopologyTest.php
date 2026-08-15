<?php

declare(strict_types=1);

use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\PubSub\Subscription as PubSubSubscription;
use Google\Rpc\Code;
use Spoolrail\Spoolrail\Exceptions\PubSubTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Spoolrail\Spoolrail\PubSub\PendingTopology;

test('requests a fresh synchronization after a transient topology failure', function (): void {
    // --- Arrange ---
    $failure = new ServiceException('Service unavailable.', Code::UNAVAILABLE);
    $subscription = Mockery::mock(PubSubSubscription::class);
    $subscription->expects('name')->once()->andReturn('projects/spoolrail/subscriptions/warehouse-orders');
    $subscription->expects('update')->once()->andThrow($failure);
    $plan = new PendingTopology;
    $plan->updateExactlyOnce($subscription, true);

    // --- Act ---
    $caught = null;

    try {
        $plan->apply();
    } catch (TopologySyncRequiresRetryException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->getPrevious()?->getPrevious())->toBe($failure);
});

test('fails topology application immediately after a permanent refusal', function (): void {
    // --- Arrange ---
    $failure = new ServiceException('Permission denied.', Code::PERMISSION_DENIED);
    $subscription = Mockery::mock(PubSubSubscription::class);
    $subscription->expects('name')->once()->andReturn('projects/spoolrail/subscriptions/warehouse-orders');
    $subscription->expects('update')->once()->andThrow($failure);
    $plan = new PendingTopology;
    $plan->updateExactlyOnce($subscription, true);

    // --- Act ---
    $caught = null;

    try {
        $plan->apply();
    } catch (PubSubTopologyException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->getPrevious())->toBe($failure);
    expect($caught?->getMessage())->toContain('updating subscription');
});
