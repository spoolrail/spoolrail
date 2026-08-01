<?php

declare(strict_types=1);

use PhpAmqpLib\Connection\AbstractConnection;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\RabbitMq\ConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\Connector;

test('maps AMQPS identity heartbeat timeouts and private CA into a verified AMQP configuration', function (): void {
    // --- Arrange ---
    $connectionConfig = new ConnectionConfig('events', [
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
    $amqpConfiguration = (new Connector)->amqpConfiguration($connectionConfig, 'rabbit.internal');

    // --- Assert ---
    expect($amqpConfiguration->getHost())->toBe('rabbit.internal');
    expect($amqpConfiguration->getPort())->toBe(5_679);
    expect($amqpConfiguration->getUser())->toBe('publisher@tenant');
    expect($amqpConfiguration->getPassword())->toBe('p@ss');
    expect($amqpConfiguration->getVhost())->toBe('orders/production');
    expect($amqpConfiguration->getConnectionName())->toBe('spoolrail:events');
    expect($amqpConfiguration->getConnectionTimeout())->toBe(7.0);
    expect($amqpConfiguration->getHeartbeat())->toBe(45);
    expect($amqpConfiguration->isKeepalive())->toBeFalse();
    expect($amqpConfiguration->getReadTimeout())->toBe(90.0);
    expect($amqpConfiguration->getWriteTimeout())->toBe(90.0);
    expect($amqpConfiguration->isSecure())->toBeTrue();
    expect($amqpConfiguration->getSslVerify())->toBeTrue();
    expect($amqpConfiguration->getSslVerifyName())->toBeTrue();
    expect($amqpConfiguration->getSslCaCert())->toBe(__FILE__);
});

test('uses TCP keepalive and a usable socket timeout when the heartbeat is zero', function (): void {
    // --- Arrange ---
    $connectionConfig = new ConnectionConfig('events', [
        'host' => 'rabbit.internal',
        'heartbeat' => 0,
    ]);

    // --- Act ---
    $amqpConfiguration = (new Connector)->amqpConfiguration($connectionConfig, 'rabbit.internal');

    // --- Assert ---
    expect($amqpConfiguration->getHeartbeat())->toBe(0);
    expect($amqpConfiguration->isKeepalive())->toBeTrue();
    expect($amqpConfiguration->getReadTimeout())->toBe(3.0);
    expect($amqpConfiguration->getWriteTimeout())->toBe(3.0);
});

test('rejects a detected RabbitMQ version older than 4.3', function (): void {
    $amqpConnection = Mockery::mock(AbstractConnection::class);
    $amqpConnection->shouldReceive('getServerProperties')
        ->once()
        ->andReturn(['version' => ['S', '4.2.9']]);
    $amqpConnection->shouldReceive('close')->once();

    expect(fn () => (new Connector)->assertSupportedVersion($amqpConnection))
        ->toThrow(
            RabbitMqTopologyException::class,
            'RabbitMQ [4.2.9] is not supported; Spoolrail requires RabbitMQ 4.3 or later.',
        );
});
