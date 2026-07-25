<?php

declare(strict_types=1);

use PhpAmqpLib\Connection\AbstractConnection;
use Spoolrail\Spoolrail\Exceptions\UnsupportedRabbitMqVersionException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionFactory;

test('maps AMQPS identity heartbeat timeouts and private CA into a verified native configuration', function (): void {
    // --- Arrange ---
    $connection = new RabbitMqConnectionConfig('events', [
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
    $config = (new RabbitMqConnectionFactory)->configuration($connection, 'rabbit.internal');

    // --- Assert ---
    expect($config->getHost())->toBe('rabbit.internal');
    expect($config->getPort())->toBe(5_679);
    expect($config->getUser())->toBe('publisher@tenant');
    expect($config->getPassword())->toBe('p@ss');
    expect($config->getVhost())->toBe('orders/production');
    expect($config->getConnectionName())->toBe('spoolrail:events');
    expect($config->getConnectionTimeout())->toBe(7.0);
    expect($config->getHeartbeat())->toBe(45);
    expect($config->isKeepalive())->toBeFalse();
    expect($config->getReadTimeout())->toBe(90.0);
    expect($config->getWriteTimeout())->toBe(90.0);
    expect($config->isSecure())->toBeTrue();
    expect($config->getSslVerify())->toBeTrue();
    expect($config->getSslVerifyName())->toBeTrue();
    expect($config->getSslCaCert())->toBe(__FILE__);
});

test('uses TCP keepalive and a usable socket timeout when the heartbeat is zero', function (): void {
    // --- Arrange ---
    $connection = new RabbitMqConnectionConfig('events', [
        'host' => 'rabbit.internal',
        'heartbeat' => 0,
    ]);

    // --- Act ---
    $config = (new RabbitMqConnectionFactory)->configuration($connection, 'rabbit.internal');

    // --- Assert ---
    expect($config->getHeartbeat())->toBe(0);
    expect($config->isKeepalive())->toBeTrue();
    expect($config->getReadTimeout())->toBe(3.0);
    expect($config->getWriteTimeout())->toBe(3.0);
});

test('rejects a detected RabbitMQ version older than 4.3', function (): void {
    $native = Mockery::mock(AbstractConnection::class);
    $native->shouldReceive('getServerProperties')
        ->once()
        ->andReturn(['version' => ['S', '4.2.9']]);
    $native->shouldReceive('close')->once();

    expect(fn () => (new RabbitMqConnectionFactory)->verifySupportedBroker($native))
        ->toThrow(
            UnsupportedRabbitMqVersionException::class,
            'RabbitMQ [4.2.9] is not supported; Spoolrail requires RabbitMQ 4.3 or later.',
        );
});
