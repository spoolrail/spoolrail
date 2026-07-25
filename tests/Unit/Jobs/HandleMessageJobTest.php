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

test('fails when its subscription is no longer registered at execution', function (): void {
    $job = new HandleMessageJob(
        Message::make('order.created', []),
        'removed-subscription',
    );
    $subscriptions = new SubscriptionRegistry;

    expect(fn () => $job->handle($subscriptions, app()))
        ->toThrow(InvalidSubscriptionException::class);
});
