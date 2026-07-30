<?php

declare(strict_types=1);

use PhpAmqpLib\Connection\AbstractConnection;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionFactory;

test('maps AMQPS identity heartbeat timeouts and private CA into a verified AMQP config', function (): void {
    // --- Arrange ---
    $connectionConfig = new RabbitMqConnectionConfig('events', [
        'scheme' => 'amqps',
        'host' => 'rabbit.internal',
        'port' => 5_679,
        'username' => 'publisher@tenant',
        'password' => 'p@ss',
        'vhost' => 'orders/production',
        'ca_file' => __FILE__,
        'connection_timeout' => 7,
        'heartbeat' => 45,
    ]);

    // --- Act ---
    $amqpConfig = (new RabbitMqConnectionFactory)->amqpConfig($connectionConfig, 'rabbit.internal');

    // --- Assert ---
    expect($amqpConfig->getHost())->toBe('rabbit.internal');
    expect($amqpConfig->getPort())->toBe(5_679);
    expect($amqpConfig->getUser())->toBe('publisher@tenant');
    expect($amqpConfig->getPassword())->toBe('p@ss');
    expect($amqpConfig->getVhost())->toBe('orders/production');
    expect($amqpConfig->getConnectionName())->toBe('spoolrail:events');
    expect($amqpConfig->getConnectionTimeout())->toBe(7.0);
    expect($amqpConfig->getHeartbeat())->toBe(45);
    expect($amqpConfig->isKeepalive())->toBeFalse();
    expect($amqpConfig->getReadTimeout())->toBe(90.0);
    expect($amqpConfig->getWriteTimeout())->toBe(90.0);
    expect($amqpConfig->isSecure())->toBeTrue();
    expect($amqpConfig->getSslVerify())->toBeTrue();
    expect($amqpConfig->getSslVerifyName())->toBeTrue();
    expect($amqpConfig->getSslCaCert())->toBe(__FILE__);
});

test('uses TCP keepalive and a usable socket timeout when the heartbeat is zero', function (): void {
    // --- Arrange ---
    $connectionConfig = new RabbitMqConnectionConfig('events', [
        'host' => 'rabbit.internal',
        'heartbeat' => 0,
    ]);

    // --- Act ---
    $amqpConfig = (new RabbitMqConnectionFactory)->amqpConfig($connectionConfig, 'rabbit.internal');

    // --- Assert ---
    expect($amqpConfig->getHeartbeat())->toBe(0);
    expect($amqpConfig->isKeepalive())->toBeTrue();
    expect($amqpConfig->getReadTimeout())->toBe(3.0);
    expect($amqpConfig->getWriteTimeout())->toBe(3.0);
});

test('rejects a detected RabbitMQ version older than 4.3', function (): void {
    $amqpConnection = Mockery::mock(AbstractConnection::class);
    $amqpConnection->shouldReceive('getServerProperties')
        ->once()
        ->andReturn(['version' => ['S', '4.2.9']]);
    $amqpConnection->shouldReceive('close')->once();

    expect(fn () => (new RabbitMqConnectionFactory)->assertSupportedVersion($amqpConnection))
        ->toThrow(
            RabbitMqTopologyException::class,
            'RabbitMQ [4.2.9] is not supported; Spoolrail requires RabbitMQ 4.3 or later.',
        );
});
