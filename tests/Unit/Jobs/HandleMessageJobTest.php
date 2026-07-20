<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

test('uses the handler currently registered for its subscription when executed', function (): void {
    // --- Arrange ---
    $message = Message::make('order.created', ['reference' => 'A-42'])
        ->withPublishedAt(CarbonImmutable::parse('2026-07-15 14:23:08.417 UTC'));
    $job = new HandleMessageJob($message, 'warehouse-orders');

    $handled = null;
    $currentHandler = Mockery::namedMock('HandleMessageJobCurrentHandler', MessageHandler::class);
    $currentHandler->shouldReceive('handle')
        ->once()
        ->andReturnUsing(function (Message $message) use (&$handled): void {
            $handled = $message;
        });
    app()->instance($currentHandler::class, $currentHandler);

    $decoyHandler = Mockery::namedMock('HandleMessageJobDecoyHandler', MessageHandler::class);
    $decoyHandler->allows('handle');
    app()->instance($decoyHandler::class, $decoyHandler);

    $subscriptions = new SubscriptionRegistry;
    $subscriptions->subscribe('returns', 'warehouse-returns', $decoyHandler::class);
    $subscriptions->subscribe('orders', 'warehouse-orders', $currentHandler::class);

    // --- Act ---
    $job->handle($subscriptions, app());

    // --- Assert ---
    expect($handled)->toEqual($message);
    $decoyHandler->shouldNotHaveReceived('handle');
});

test('uses the replacement subscription for a message queued under its former name', function (): void {
    // --- Arrange ---
    $message = Message::make('order.created', ['reference' => 'A-42']);
    $job = new HandleMessageJob($message, 'warehouse-order-processing');

    $handled = null;
    $replacementHandler = Mockery::namedMock('HandleMessageJobReplacementHandler', MessageHandler::class);
    $replacementHandler->shouldReceive('handle')
        ->once()
        ->andReturnUsing(function (Message $message) use (&$handled): void {
            $handled = $message;
        });
    app()->instance($replacementHandler::class, $replacementHandler);

    $subscriptions = new SubscriptionRegistry;
    $subscriptions
        ->subscribe('orders', 'warehouse-order-processing-v2', $replacementHandler::class)
        ->drainMessagesQueuedFor('warehouse-order-processing');

    // --- Act ---
    $job->handle($subscriptions, app());

    // --- Assert ---
    expect($handled)->toBe($message);
});

test('fails when its subscription is no longer registered at execution', function (): void {
    // --- Arrange ---
    $job = new HandleMessageJob(
        Message::make('order.created', []),
        'removed-subscription',
    );
    $subscriptions = new SubscriptionRegistry;

    // --- Act ---
    $failure = null;

    try {
        $job->handle($subscriptions, app());
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(InvalidSubscriptionException::class);
});
