<?php

declare(strict_types=1);

use Illuminate\Contracts\Debug\ExceptionHandler;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionProcess;

test('reports a child failure with its original exception', function (): void {
    // --- Arrange ---
    $failure = new RuntimeException('Broker unavailable.');
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('consume')
        ->with('warehouse-orders')
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
        'subscription' => 'warehouse-orders',
    ])->run();

    // --- Assert ---
    expect($exitCode)->toBe(SubscriptionProcess::REPORTED_FAILURE_EXIT_CODE);
    expect($reported?->getPrevious())->toBe($failure);
});

test('reports an unexpected return from the receive loop', function (): void {
    // --- Arrange ---
    $consumer = Mockery::mock(SubscriptionConsumer::class);
    $consumer->expects('consume')->with('warehouse-orders');
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
    $this->artisan('spoolrail:consume', [
        'subscription' => 'warehouse-orders',
    ])->assertFailed();

    // --- Assert ---
    expect($reported?->getMessage())
        ->toBe('Spoolrail subscription [warehouse-orders] stopped consuming unexpectedly.');
});
