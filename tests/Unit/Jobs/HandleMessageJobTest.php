<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Mockery\MockInterface;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\TransportContext;

test('uses the handler currently registered for its subscription when executed', function (): void {
    // --- Arrange ---
    $message = Message::make('order.created', ['reference' => 'A-42'])
        ->withPublishedAt(CarbonImmutable::parse('2026-07-15 14:23:08.417 UTC'));
    $job = new HandleMessageJob($message, 'warehouse-orders');

    $handled = null;
    $currentHandler = createNamedMessageHandlerMock('HandleMessageJobCurrentHandler');
    $currentHandler->shouldReceive('handle')
        ->once()
        ->andReturnUsing(function (Message $message) use (&$handled): void {
            $handled = $message;
        });
    app()->instance($currentHandler::class, $currentHandler);

    $decoyHandler = createNamedMessageHandlerMock('HandleMessageJobDecoyHandler');
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

test('retains transport context when the universal job is serialized', function (): void {
    // --- Arrange ---
    $transport = new TransportContext(
        driver: 'rabbitmq',
        connectionName: 'rabbitmq',
        topic: 'orders',
        subscription: 'warehouse-orders',
        headers: [
            'correlation-id' => 'A-42',
            'transport-added' => 7,
        ],
        redelivered: true,
    );
    $message = Message::make('order.created', ['reference' => 'A-42'])
        ->withPublishedAt(CarbonImmutable::parse('2026-07-15 14:23:08.417 UTC'))
        ->withTransport($transport);
    $job = new HandleMessageJob($message, 'warehouse-orders');

    // --- Act ---
    $restored = unserialize(serialize($job));

    // --- Assert ---
    expect($restored)->toBeInstanceOf(HandleMessageJob::class);
    expect($restored->message->transport)->toEqual($transport);
    expect($restored->message->transport)->not->toBe($transport);
});

function createNamedMessageHandlerMock(string $name): MessageHandler&MockInterface
{
    /** @var MessageHandler&MockInterface $handler */
    $handler = Mockery::namedMock($name, MessageHandler::class);

    return $handler;
}
