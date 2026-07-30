<?php

declare(strict_types=1);

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPBasicCancelException;
use PhpAmqpLib\Exception\AMQPHeartbeatMissedException;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Drivers\RabbitMqDriver;
use Spoolrail\Spoolrail\Exceptions\RabbitMqConsumerCancelledException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqPublicationRejectedException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqQueueNameTooLongException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopicNameTooLongException;
use Spoolrail\Spoolrail\OwnershipPrefix;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionFactory;

test('publishes a persistent message and waits for its confirmation', function (): void {
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
    $native->expects('close')
        ->once()
        ->andThrow(new RuntimeException('Closing the uncertain connection failed.'));
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

test('rejects an over-limit topic before opening a connection', function (): void {
    $factory = Mockery::mock(RabbitMqConnectionFactory::class);
    $factory->shouldNotReceive('create');

    expect(fn () => rabbitMqDriver($factory)->publish(str_repeat('a', 256), '{}'))
        ->toThrow(RabbitMqTopicNameTooLongException::class);
});

test('rejects an over-limit queue before opening a connection', function (): void {
    config()->set('spoolrail.prefix', 'a'.str_repeat('b', 249));

    $factory = Mockery::mock(RabbitMqConnectionFactory::class);
    $factory->shouldNotReceive('create');

    expect(fn () => rabbitMqDriver($factory)->consume('orders', static function (): void {}))
        ->toThrow(RabbitMqQueueNameTooLongException::class);
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

            throw new LogicException('Negative acknowledgement handler returned.');
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

    $driver->publish('orders', '{"message":"second"}');
    $driver->close();
});

test('acknowledges a delivery only after the handoff returns', function (): void {
    // --- Arrange ---
    $events = [];
    $expectedQueue = app(OwnershipPrefix::class)->current().'-order-imports';
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
        ) use ($channel, $expectedQueue): bool {
            expect($queue)->toBe($expectedQueue);
            expect($noAck)->toBeFalse();
            expect($exclusive)->toBeFalse();

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

    $driver = rabbitMqDriver($factory, prefetch: 23);

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

test('propagates consumer transport failures and discards the connection', function (Throwable $failure): void {
    // --- Arrange ---
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
})->with([
    'missed heartbeat' => fn (): AMQPHeartbeatMissedException => new AMQPHeartbeatMissedException('Missed server heartbeat.'),
    'broker cancellation' => fn (): AMQPBasicCancelException => new AMQPBasicCancelException('consumer-tag'),
]);

test('treats the consuming loop ending as an unexpected cancellation', function (): void {
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

    expect(fn () => $driver->consume('order-imports', static function (): void {}))
        ->toThrow(RabbitMqConsumerCancelledException::class);
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
): RabbitMqDriver {
    $config = [
        'publisher_confirm_timeout' => $publisherConfirmTimeout,
        'prefetch' => $prefetch,
    ];

    return new RabbitMqDriver(
        new RabbitMqConnectionConfig('rabbitmq', $config),
        $factory,
        Mockery::mock(ManagedTopology::class),
        app(OwnershipPrefix::class),
    );
}
