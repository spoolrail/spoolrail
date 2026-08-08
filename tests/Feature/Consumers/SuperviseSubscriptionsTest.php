<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\ConsumerSupervisor;
use Spoolrail\Spoolrail\Subscriptions\StartSubscriptionProcess;
use Spoolrail\Spoolrail\Subscriptions\TerminationSignal;

beforeEach(function (): void {
    RecordingMessageHandler::$messages = [];
});

test('supervises every active subscription on the default connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.default', 'events');
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);
    config()->set('spoolrail.connections.partner', ['driver' => 'recording']);

    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'partner-orders', RecordingMessageHandler::class)
        ->onConnection('partner');

    $supervisor = Mockery::mock(ConsumerSupervisor::class);
    $supervisor->expects('supervise')
        ->with(['warehouse-orders', 'billing-orders'], Mockery::type(Closure::class))
        ->andReturnTrue();
    app()->instance(ConsumerSupervisor::class, $supervisor);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('supervises subscriptions selected by a named connection', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);

    Spoolrail::subscribe('orders', 'default-orders', RecordingMessageHandler::class);
    Spoolrail::subscribe('orders', 'event-orders', RecordingMessageHandler::class)
        ->onConnection('events');

    $supervisor = Mockery::mock(ConsumerSupervisor::class);
    $supervisor->expects('supervise')
        ->with(['event-orders'], Mockery::type(Closure::class))
        ->andReturnTrue();
    app()->instance(ConsumerSupervisor::class, $supervisor);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail --connection=events')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('supervises one non-array subscription through the same runtime', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);

    Spoolrail::subscribe('orders', 'event-orders', RecordingMessageHandler::class)
        ->onConnection('events');

    $supervisor = Mockery::mock(ConsumerSupervisor::class);
    $supervisor->expects('supervise')
        ->with(['event-orders'], Mockery::type(Closure::class))
        ->andReturnTrue();
    app()->instance(ConsumerSupervisor::class, $supervisor);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail event-orders')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
});

test('keeps explicit array subscription consumption in process', function (): void {
    // --- Arrange ---
    config()->set('queue.default', 'sync');

    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);
    Spoolrail::publish('orders', Message::make('order.created', ['order_id' => 42]));

    $supervisor = Mockery::mock(ConsumerSupervisor::class);
    $supervisor->shouldNotReceive('supervise');
    app()->instance(ConsumerSupervisor::class, $supervisor);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail warehouse-orders')->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect(RecordingMessageHandler::$messages)->toHaveCount(1);
    expect(RecordingMessageHandler::$messages[0]->payload)->toBe(['order_id' => 42]);
});

test('rejects a connection option when selecting one subscription', function (): void {
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);

    expect(fn () => $this->artisan('spoolrail warehouse-orders --connection=array')->run())
        ->toThrow(
            ConsumerException::class,
            'The [--connection] option cannot be used when a subscription is selected.',
        );
});

test('rejects all-subscription supervision for an array connection', function (): void {
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class);

    expect(fn () => $this->artisan('spoolrail')->run())
        ->toThrow(
            ConsumerException::class,
            'Spoolrail cannot supervise every subscription on the in-process [array] connection. Select one subscription explicitly.',
        );
});

test('rejects a connection without active subscriptions', function (): void {
    config()->set('spoolrail.connections.events', ['driver' => 'recording']);

    expect(fn () => $this->artisan('spoolrail --connection=events')->run())
        ->toThrow(
            ConsumerException::class,
            'Spoolrail connection [events] has no active subscriptions.',
        );
});

test('preflights lazy driver configuration before process supervision', function (): void {
    // --- Arrange ---
    config()->set('spoolrail.outbox.enabled', true);
    config()->set('spoolrail.connections.events', [
        'driver' => 'rabbitmq',
        'scheme' => 'https',
    ]);

    Spoolrail::subscribe('orders', 'event-orders', RecordingMessageHandler::class)
        ->onConnection('events');

    $start = Mockery::mock(StartSubscriptionProcess::class);
    $start->expects('ensureSupported');
    $start->shouldNotReceive('__invoke');
    app()->instance(StartSubscriptionProcess::class, $start);

    $termination = Mockery::mock(TerminationSignal::class);
    $termination->shouldNotReceive('current');
    app()->instance(TerminationSignal::class, $termination);

    // --- Act / Assert ---
    expect(fn () => $this->artisan('spoolrail event-orders')->run())
        ->toThrow(InvalidConfigException::class, 'setting [scheme] must be [amqp] or [amqps]');
});

class RecordingMessageHandler implements MessageHandler
{
    /** @var list<Message> */
    public static array $messages = [];

    public function handle(Message $message): void
    {
        self::$messages[] = $message;
    }
}
