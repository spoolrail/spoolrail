<?php

declare(strict_types=1);

use Illuminate\Config\Repository;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPBasicCancelException;
use PhpAmqpLib\Exception\AMQPHeartbeatMissedException;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Drivers\RabbitMqDriver;
use Spoolrail\Spoolrail\Exceptions\RabbitMqConsumerCancelledException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqPublicationRejectedException;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionFactory;

test('publishes a persistent message and waits for its confirmation', function (): void {
    // --- Arrange ---
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->expects('confirm_select')->once();
    $channel->expects('set_nack_handler')->once();
    $channel->expects('basic_publish')
        ->once()
        ->withArgs(fn (AMQPMessage $message, string $exchange): bool => $message->getBody() === '{"message":"accepted"}'
            && $message->get('content_type') === 'application/json'
            && $message->get('delivery_mode') === AMQPMessage::DELIVERY_MODE_PERSISTENT
            && $exchange === 'orders');
    $channel->expects('wait_for_pending_acks')->once()->with(17);

    $driver = rabbitMqDriver($factory, publisherConfirmTimeout: 17);

    // --- Act ---
    $driver->publish('orders', '{"message":"accepted"}');
    $driver->close();
});

test('propagates a confirmation timeout, discards the uncertain connection, and does not retry', function (): void {
    // --- Arrange ---
    $failure = new AMQPTimeoutException('Confirmation timed out.', 9);
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->allows('confirm_select');
    $channel->allows('set_nack_handler');
    $channel->allows('basic_publish');
    $channel->expects('wait_for_pending_acks')->once()->with(9)->andThrow($failure);

    $driver = rabbitMqDriver($factory, publisherConfirmTimeout: 9);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', '{}');
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
});

test('propagates a connection failure before publishing and does not retry', function (): void {
    // --- Arrange ---
    $failure = new AMQPIOException(
        'stream_socket_client(): SSL operation failed: certificate verify failed',
    );
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $factory->expects('create')->once()->andThrow($failure);

    $driver = rabbitMqDriver($factory);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', '{}');
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
});

test('turns a negative publisher confirmation into a publication rejection', function (): void {
    // --- Arrange ---
    $nack = null;
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->allows('confirm_select');
    $channel->expects('set_nack_handler')
        ->once()
        ->andReturnUsing(function (Closure $handler) use (&$nack): void {
            $nack = $handler;
        });
    $channel->allows('basic_publish');
    $channel->expects('wait_for_pending_acks')
        ->once()
        ->andReturnUsing(function () use (&$nack): never {
            expect($nack)->toBeInstanceOf(Closure::class);

            $nack();
        });

    $driver = rabbitMqDriver($factory);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', '{}');
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBeInstanceOf(RabbitMqPublicationRejectedException::class);
});

test('refreshes an idle publisher connection before publishing again', function (): void {
    // --- Arrange ---
    $firstChannel = Mockery::mock(AMQPChannel::class);
    $secondChannel = Mockery::mock(AMQPChannel::class);
    $firstNative = Mockery::mock(AbstractConnection::class);
    $secondNative = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $factory->expects('create')->twice()->andReturn($firstNative, $secondNative);
    $firstNative->expects('channel')->once()->andReturn($firstChannel);
    $firstNative->expects('getHeartbeat')->once()->andReturn(5);
    $firstNative->expects('getLastActivity')->once()->andReturn(microtime(true) - 11);
    $firstNative->expects('close')->once();
    $secondNative->expects('channel')->once()->andReturn($secondChannel);
    $secondNative->expects('close')->once();

    foreach ([
        [$firstChannel, '{"message":"first"}'],
        [$secondChannel, '{"message":"second"}'],
    ] as [$channel, $body]) {
        $channel->expects('confirm_select')->once();
        $channel->expects('set_nack_handler')->once();
        $channel->expects('basic_publish')
            ->once()
            ->withArgs(static fn (AMQPMessage $message, string $exchange): bool => $message->getBody() === $body
                && $exchange === 'orders');
        $channel->expects('wait_for_pending_acks')->once()->with(60);
    }

    $driver = rabbitMqDriver($factory);
    $driver->publish('orders', '{"message":"first"}');

    // --- Act ---
    $driver->publish('orders', '{"message":"second"}');
    $driver->close();
});

test('acknowledges a delivery only after the handoff returns', function (): void {
    // --- Arrange ---
    $events = [];
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->expects('basic_qos')->once()->with(0, 23, false);
    $channel->expects('basic_consume')
        ->once()
        ->withArgs(function (
            string $queue,
            string $consumerTag,
            bool $noLocal,
            bool $noAck,
            bool $exclusive,
            bool $noWait,
            Closure $callback,
            mixed $ticket,
            AMQPTable $arguments,
        ) use ($channel): bool {
            expect($queue)->toBe('warehouse-production-order-imports');
            expect([$consumerTag, $noLocal, $noAck, $exclusive, $noWait, $ticket])
                ->toBe(['', false, false, false, false, null]);
            expect($arguments->getNativeData())->toBe(['x-consumer-timeout' => 45_000]);

            $delivery = new AMQPMessage('message body');
            $delivery->setChannel($channel);
            $delivery->setDeliveryInfo(1, false, 'orders', '');
            $callback($delivery);

            return true;
        });
    $channel->expects('basic_ack')
        ->once()
        ->with(1, false)
        ->andReturnUsing(function () use (&$events): void {
            $events[] = 'ack';
        });
    $channel->expects('consume')->once();

    $driver = rabbitMqDriver(
        $factory,
        prefetch: 23,
        consumerAcknowledgementTimeout: 45,
    );

    // --- Act ---
    try {
        $driver->consume('order-imports', function (string $body) use (&$events): void {
            expect($body)->toBe('message body');
            $events[] = 'handoff';
        });
    } catch (RabbitMqConsumerCancelledException) {
    }

    // --- Assert ---
    expect($events)->toBe(['handoff', 'ack']);
});

test('omits the consumer acknowledgement timeout argument when it is not configured', function (): void {
    // --- Arrange ---
    $arguments = null;
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->allows('basic_qos');
    $channel->expects('basic_consume')
        ->once()
        ->andReturnUsing(function (
            mixed $_queue,
            mixed $_consumerTag,
            mixed $_noLocal,
            mixed $_noAck,
            mixed $_exclusive,
            mixed $_noWait,
            mixed $_callback,
            mixed $_ticket,
            AMQPTable $consumerArguments,
        ) use (&$arguments): string {
            $arguments = $consumerArguments->getNativeData();

            return 'consumer';
        });
    $channel->expects('consume')->once();

    $driver = rabbitMqDriver($factory);

    // --- Act ---
    try {
        $driver->consume('order-imports', static function (): void {});
    } catch (RabbitMqConsumerCancelledException) {
    }

    // --- Assert ---
    expect($arguments)->toBe([]);
});

test('propagates a missed consumer heartbeat and discards the connection', function (): void {
    // --- Arrange ---
    $failure = new AMQPHeartbeatMissedException('Missed server heartbeat.');
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->allows('basic_qos');
    $channel->allows('basic_consume');
    $channel->expects('consume')->once()->andThrow($failure);

    $driver = rabbitMqDriver($factory);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('order-imports', static function (): void {});
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
});

test('propagates broker cancellation and discards the connection', function (): void {
    // --- Arrange ---
    $cancellation = new AMQPBasicCancelException('consumer-tag');
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->allows('basic_qos');
    $channel->allows('basic_consume');
    $channel->expects('consume')->once()->andThrow($cancellation);

    $driver = rabbitMqDriver($factory);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('order-imports', static function (): void {});
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($cancellation);
});

test('preserves a handoff exception that has the same type as broker cancellation', function (): void {
    // --- Arrange ---
    $failure = new AMQPBasicCancelException('application failure');
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->allows('basic_qos');
    $channel->expects('basic_consume')
        ->once()
        ->andReturnUsing(function (
            mixed $_queue,
            mixed $_consumerTag,
            mixed $_noLocal,
            mixed $_noAck,
            mixed $_exclusive,
            mixed $_noWait,
            Closure $callback,
        ): string {
            $callback(new AMQPMessage('message body'));

            return 'consumer';
        });
    $channel->shouldNotReceive('consume');

    $driver = rabbitMqDriver($factory);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('order-imports', static function () use ($failure): never {
            throw $failure;
        });
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
});

test('treats the consuming loop ending as an unexpected cancellation', function (): void {
    // --- Arrange ---
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->allows('basic_qos');
    $channel->allows('basic_consume');
    $channel->expects('consume')->once();

    $driver = rabbitMqDriver($factory);

    // --- Act & Assert ---
    expect(fn () => $driver->consume('order-imports', static function (): void {}))
        ->toThrow(RabbitMqConsumerCancelledException::class);
});

test('leaves a failed handoff unsettled, discards the connection, and propagates the same failure', function (): void {
    // --- Arrange ---
    $failure = new RuntimeException('Laravel Queue handoff failed.');
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->allows('basic_qos');
    $channel->expects('basic_consume')
        ->once()
        ->andReturnUsing(function (
            mixed $_queue,
            mixed $_consumerTag,
            mixed $_noLocal,
            mixed $_noAck,
            mixed $_exclusive,
            mixed $_noWait,
            Closure $callback,
        ) use ($channel): string {
            $delivery = new AMQPMessage('message body');
            $delivery->setChannel($channel);
            $delivery->setDeliveryInfo(1, false, 'orders', '');
            $callback($delivery);

            return 'consumer';
        });
    $channel->shouldNotReceive('basic_ack');

    $driver = rabbitMqDriver($factory);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('order-imports', static function () use ($failure): never {
            throw $failure;
        });
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
});

test('stops consumption and discards the connection when acknowledging fails', function (): void {
    // --- Arrange ---
    $failure = new RuntimeException('Acknowledgement failed.');
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $factory->expects('create')->once()->andReturn($native);
    $channel->allows('basic_qos');
    $channel->expects('basic_consume')
        ->once()
        ->andReturnUsing(function (
            mixed $_queue,
            mixed $_consumerTag,
            mixed $_noLocal,
            mixed $_noAck,
            mixed $_exclusive,
            mixed $_noWait,
            Closure $callback,
        ) use ($channel): string {
            $delivery = new AMQPMessage('message body');
            $delivery->setChannel($channel);
            $delivery->setDeliveryInfo(1, false, 'orders', '');
            $callback($delivery);

            return 'consumer';
        });
    $channel->expects('basic_ack')->once()->with(1, false)->andThrow($failure);
    $channel->shouldNotReceive('basic_nack');
    $channel->shouldNotReceive('basic_reject');

    $driver = rabbitMqDriver($factory);

    // --- Act ---
    $caught = null;

    try {
        $driver->consume('order-imports', static function (): void {});
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBe($failure);
});

function rabbitMqDriver(
    RabbitMqConnectionFactory $factory,
    int $publisherConfirmTimeout = 60,
    int $prefetch = 10,
    ?int $consumerAcknowledgementTimeout = null,
): RabbitMqDriver {
    $configuration = [
        'url' => 'amqp://user:secret@localhost/vhost',
        'publisher_confirm_timeout' => $publisherConfirmTimeout,
        'prefetch' => $prefetch,
    ];

    if ($consumerAcknowledgementTimeout !== null) {
        $configuration['consumer_ack_timeout'] = $consumerAcknowledgementTimeout;
    }

    return new RabbitMqDriver(
        new RabbitMqConnectionConfig('rabbitmq', $configuration),
        $factory,
        Mockery::mock(ManagedTopology::class),
        new OwnershipPrefix(new Repository([
            'spoolrail' => ['prefix' => 'warehouse-production'],
        ])),
    );
}
