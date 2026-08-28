<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPHeartbeatMissedException;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Drivers\RabbitMqDriver;
use Spoolrail\Spoolrail\Enums\ConsumptionFailure;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\RabbitMq\ConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\Connector;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

test('publishes a persistent message and waits for its confirmation', function (): void {
    $body = rabbitMqMessageBody('accepted');
    $headers = [
        'correlation-id' => 'A-42',
        'traceparent' => '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
    ];
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $connector = Mockery::mock(Connector::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $connector->expects('connect')->once()->andReturn($native);
    $channel->expects('confirm_select')->once();
    $channel->expects('set_nack_handler')->once();
    $channel->expects('basic_publish')
        ->once()
        ->withArgs(fn (AMQPMessage $message, string $exchange): bool => $message->getBody() === $body
            && $message->get('content_type') === 'application/json'
            && $message->get('delivery_mode') === AMQPMessage::DELIVERY_MODE_PERSISTENT
            && $message->get('message_id') === '01890a5d-ac96-774b-bcd0-48f622f3e798'
            && $message->get('type') === 'order.created'
            && $message->get('timestamp') === CarbonImmutable::parse('2026-07-15T14:23:08.417Z')->getTimestamp()
            && $message->get('application_headers')->getNativeData() === $headers
            && $exchange === 'orders');
    $channel->expects('wait_for_pending_acks')->once()->with(17);

    $driver = rabbitMqDriver($connector, publisherConfirmTimeout: 17);

    $driver->publish('orders', $body, $headers, 'order:42');
    $driver->close();
});

test('reports an unknown publication outcome when confirmation times out without retrying', function (): void {
    // --- Arrange ---
    $failure = new AMQPTimeoutException('Confirmation timed out.', 9);
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $connector = Mockery::mock(Connector::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')
        ->once()
        ->andThrow(new RuntimeException('Closing the uncertain connection failed.'));
    $connector->expects('connect')->once()->andReturn($native);
    $channel->allows('confirm_select');
    $channel->allows('set_nack_handler');
    $channel->allows('basic_publish');
    $channel->expects('wait_for_pending_acks')->once()->with(9)->andThrow($failure);

    $driver = rabbitMqDriver($connector, publisherConfirmTimeout: 9);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', rabbitMqMessageBody(), []);
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBeInstanceOf(PublicationException::class);
    expect($caught?->outcome)->toBe(PublicationOutcome::Unknown);
    expect($caught?->getPrevious())->toBe($failure);
});

test('reports that a publication was not sent when connecting fails without retrying', function (): void {
    // --- Arrange ---
    $failure = new AMQPIOException(
        'stream_socket_client(): SSL operation failed: certificate verify failed',
    );
    $connector = Mockery::mock(Connector::class);

    $connector->expects('connect')->once()->andThrow($failure);

    $driver = rabbitMqDriver($connector);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', rabbitMqMessageBody(), []);
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBeInstanceOf(PublicationException::class);
    expect($caught?->outcome)->toBe(PublicationOutcome::NotSent);
    expect($caught?->getPrevious())->toBe($failure);
});

test('preserves a package-classified failure before publishing', function (): void {
    // --- Arrange ---
    $failure = RabbitMqTopologyException::unsupportedVersion('4.2.9');
    $connector = Mockery::mock(Connector::class);

    $connector->expects('connect')->once()->andThrow($failure);

    $driver = rabbitMqDriver($connector);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', rabbitMqMessageBody(), []);
    } catch (Throwable $caught) {
    }

    // --- Assert ---
    expect($caught ?? null)->toBe($failure);
});

test('rejects an over-limit topic before opening a connection', function (): void {
    $connector = Mockery::mock(Connector::class);
    $connector->shouldNotReceive('connect');

    expect(fn () => rabbitMqDriver($connector)->publish(
        str_repeat('a', 256),
        rabbitMqMessageBody(),
        [],
    ))
        ->toThrow(LengthException::class);
});

test('requires an ownership prefix before opening a consumer connection', function (): void {
    config()->set('spoolrail.prefix');

    $connector = Mockery::mock(Connector::class);
    $connector->shouldNotReceive('connect');

    expect(fn () => rabbitMqDriver($connector)->receive(
        'orders',
        static function (): void {},
        static function (): void {},
    ))
        ->toThrow(InvalidConfigException::class);
});

test('turns a negative publisher confirmation into a publication rejection', function (): void {
    // --- Arrange ---
    $nack = null;
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $connector = Mockery::mock(Connector::class);

    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $connector->expects('connect')->once()->andReturn($native);
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

    $driver = rabbitMqDriver($connector);

    // --- Act ---
    $caught = null;

    try {
        $driver->publish('orders', rabbitMqMessageBody(), []);
    } catch (Throwable $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught)->toBeInstanceOf(PublicationException::class);
    expect($caught?->outcome)->toBe(PublicationOutcome::Rejected);
    expect($caught?->getPrevious())->toBeNull();
});

test('refreshes an idle publisher connection before publishing again', function (): void {
    $firstChannel = Mockery::mock(AMQPChannel::class);
    $secondChannel = Mockery::mock(AMQPChannel::class);
    $firstNative = Mockery::mock(AbstractConnection::class);
    $secondNative = Mockery::mock(AbstractConnection::class);
    $connector = Mockery::mock(Connector::class);

    $connector->expects('connect')->twice()->andReturn($firstNative, $secondNative);
    $firstNative->expects('channel')->once()->andReturn($firstChannel);
    $firstNative->expects('getHeartbeat')->once()->andReturn(5);
    $firstNative->expects('getLastActivity')->once()->andReturn(microtime(true) - 11);
    $firstNative->expects('close')->once();
    $secondNative->expects('channel')->once()->andReturn($secondChannel);
    $secondNative->expects('close')->once();

    foreach ([
        [$firstChannel, rabbitMqMessageBody('first')],
        [$secondChannel, rabbitMqMessageBody('second')],
    ] as [$channel, $body]) {
        $channel->expects('confirm_select')->once();
        $channel->expects('set_nack_handler')->once();
        $channel->expects('basic_publish')
            ->once()
            ->withArgs(static fn (AMQPMessage $message, string $exchange): bool => $message->getBody() === $body
                && $exchange === 'orders');
        $channel->expects('wait_for_pending_acks')->once()->with(60);
    }

    $driver = rabbitMqDriver($connector);
    $driver->publish('orders', rabbitMqMessageBody('first'), []);

    $driver->publish('orders', rabbitMqMessageBody('second'), []);
    $driver->close();
});

test('receives multiple subscriptions through one RabbitMQ channel', function (): void {
    // --- Arrange ---
    $callbacks = [];
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $connector = Mockery::mock(Connector::class);
    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $connector->expects('connect')->once()->andReturn($native);
    $channel->expects('basic_qos')->once()->with(0, 23, false);
    $channel->expects('basic_consume')
        ->twice()
        ->andReturnUsing(function (string $queue, mixed ...$arguments) use (&$callbacks): string {
            $callbacks[$queue] = $arguments[5];

            return $queue;
        });
    $channel->expects('wait')->once()->andReturnUsing(
        function () use (&$callbacks, $channel): void {
            foreach ($callbacks as $queue => $callback) {
                $delivery = new AMQPMessage("body:$queue", [
                    'message_id' => "id:$queue",
                    'timestamp' => 1_784_112_188,
                    'application_headers' => new AMQPTable(['correlation-id' => 'A-42']),
                ]);
                $delivery->setChannel($channel);
                $delivery->setDeliveryInfo(count($callbacks), true, 'orders', '');
                $callback($delivery);
            }
        },
    );
    $driver = rabbitMqDriver($connector, prefetch: 23);
    $received = [];

    // --- Act ---
    foreach (['order-imports', 'billing-orders'] as $subscription) {
        $driver->receive($subscription, function (array $deliveries) use ($subscription, &$received): void {
            $received[$subscription] = $deliveries[0];
        }, static function (): void {});
    }
    $driver->waitForConsumerIo();
    $driver->close();

    // --- Assert ---
    expect($received)->toHaveKeys(['order-imports', 'billing-orders']);
    expect($received['order-imports']->body)->toContain('order-imports');
    expect($received['order-imports']->headers)->toBe(['correlation-id' => 'A-42']);
    expect($received['order-imports']->transportMessageId)->toContain('order-imports');
    expect($received['order-imports']->redelivered)->toBeTrue();
});

test('buffers prefetched RabbitMQ messages behind logical receive attempts', function (): void {
    // --- Arrange ---
    $callback = null;
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $connector = Mockery::mock(Connector::class);
    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $connector->expects('connect')->once()->andReturn($native);
    $channel->allows('basic_qos');
    $channel->expects('basic_consume')
        ->once()
        ->andReturnUsing(function (string $queue, mixed ...$arguments) use (&$callback): string {
            $callback = $arguments[5];

            return $queue;
        });
    $channel->expects('wait')->once()->andReturnUsing(
        function () use (&$callback, $channel): void {
            assert($callback instanceof Closure);

            foreach (['first', 'second'] as $tag => $body) {
                $message = new AMQPMessage($body);
                $message->setChannel($channel);
                $message->setDeliveryInfo($tag + 1, false, 'orders', '');
                $callback($message);
            }
        },
    );
    $driver = rabbitMqDriver($connector);
    $received = [];

    // --- Act ---
    $driver->receive('order-imports', function (array $deliveries) use (&$received): void {
        $received[] = $deliveries[0]->body;
    }, static function (): void {});
    $driver->waitForConsumerIo();
    $driver->receive('order-imports', function (array $deliveries) use (&$received): void {
        $received[] = $deliveries[0]->body;
    }, static function (): void {});
    $driver->close();

    // --- Assert ---
    expect($received)->toBe(['first', 'second']);
});

test('acknowledges and releases RabbitMQ deliveries through native settlement writes', function (): void {
    // --- Arrange ---
    $channel = Mockery::mock(AMQPChannel::class);
    $first = new AMQPMessage('acknowledge');
    $first->setChannel($channel);
    $first->setDeliveryInfo(1, false, 'orders', '');
    $second = new AMQPMessage('release');
    $second->setChannel($channel);
    $second->setDeliveryInfo(2, false, 'orders', '');
    $channel->expects('basic_ack')->once()->with(1, false);
    $channel->expects('basic_nack')->once()->with(2, false, true);
    $driver = rabbitMqDriver(Mockery::mock(Connector::class));
    $acknowledged = false;
    $released = false;

    // --- Act ---
    $driver->acknowledge(
        new Delivery('acknowledge', $first),
        function () use (&$acknowledged): void {
            $acknowledged = true;
        },
        static function (): void {},
    );
    $driver->release(
        new Delivery('release', $second),
        function () use (&$released): void {
            $released = true;
        },
        static function (): void {},
    );

    // --- Assert ---
    expect($acknowledged)->toBeTrue();
    expect($released)->toBeTrue();
});

test('reports a shared RabbitMQ reactor failure and discards the connection', function (): void {
    // --- Arrange ---
    $failure = new AMQPHeartbeatMissedException('Missed server heartbeat.');
    $channel = Mockery::mock(AMQPChannel::class);
    $native = Mockery::mock(AbstractConnection::class);
    $connector = Mockery::mock(Connector::class);
    $native->expects('channel')->once()->andReturn($channel);
    $native->expects('close')->once();
    $connector->expects('connect')->once()->andReturn($native);
    $channel->allows('basic_qos');
    $channel->allows('basic_consume');
    $channel->expects('wait')->once()->andThrow($failure);
    $driver = rabbitMqDriver($connector);
    $driver->receive('order-imports', static function (): void {}, static function (): void {});

    // --- Act / Assert ---
    expect(fn () => $driver->waitForConsumerIo())
        ->toThrow(function (ConsumptionException $exception) use ($failure): void {
            expect($exception->failure)->toBe(ConsumptionFailure::ConsumerStopped);
            expect($exception->getPrevious())->toBe($failure);
        });
});

function rabbitMqDriver(
    Connector $connector,
    int $publisherConfirmTimeout = 60,
    int $prefetch = 10,
): RabbitMqDriver {
    $config = [
        'publisher_confirm_timeout' => $publisherConfirmTimeout,
        'prefetch' => $prefetch,
    ];

    return new RabbitMqDriver(
        new ConnectionConfig('rabbitmq', $config),
        $connector,
        Mockery::mock(CanManageTopology::class),
        app(OwnershipPrefix::class),
    );
}

function rabbitMqMessageBody(string $reference = 'A-42'): string
{
    return json_encode([
        'id' => '01890a5d-ac96-774b-bcd0-48f622f3e798',
        'type' => 'order.created',
        'payload' => ['reference' => $reference],
        'published_at' => '2026-07-15T14:23:08.417Z',
    ], JSON_THROW_ON_ERROR);
}
