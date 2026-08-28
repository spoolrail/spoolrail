<?php

declare(strict_types=1);

use Illuminate\Contracts\Debug\ExceptionHandler;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Subscriptions\ConsumerProcess;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;

test('reports a shared child failure with its original exception', function (): void {
    // --- Arrange ---
    $failure = new RuntimeException('Broker unavailable.');
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('consume')
        ->with(['warehouse-orders', 'billing-orders'])
        ->andThrow($failure);
    app()->instance(SubscriptionConsumer::class, $consumer);

    $reported = null;
    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->expects('report')
        ->with(Mockery::type(ConsumerException::class))
        ->andReturnUsing(function (ConsumerException $exception) use (&$reported): void {
            $reported = $exception;
        });
    app()->instance(ExceptionHandler::class, $exceptions);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:consume', [
        'subscriptions' => ['warehouse-orders', 'billing-orders'],
    ])->run();

    // --- Assert ---
    expect($exitCode)->toBe(ConsumerProcess::REPORTED_FAILURE_EXIT_CODE);
    expect($reported?->getPrevious())->toBe($failure);
    expect($reported?->getMessage())->toContain('warehouse-orders, billing-orders');
});

test('returns successfully after graceful grouped runtime shutdown', function (): void {
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('consume')->with(['warehouse-orders']);
    app()->instance(SubscriptionConsumer::class, $consumer);

    $exceptions = Mockery::mock(ExceptionHandler::class);
    $exceptions->shouldNotReceive('report');
    app()->instance(ExceptionHandler::class, $exceptions);

    $this->artisan('spoolrail:consume', [
        'subscriptions' => ['warehouse-orders'],
    ])->assertSuccessful();
});
