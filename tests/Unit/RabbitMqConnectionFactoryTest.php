<?php

declare(strict_types=1);

use PhpAmqpLib\Connection\AbstractConnection;
use Spoolrail\Spoolrail\Exceptions\UnsupportedRabbitMqVersionException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionFactory;

test('maps AMQPS identity heartbeat timeouts and private CA into a verified native configuration', function (): void {
    // --- Arrange ---
    $connection = new RabbitMqConnectionConfig('events', [
        'url' => 'amqps://publisher%40tenant:p%40ss@rabbit.internal:5679/orders%2Fproduction',
        'ca_file' => __FILE__,
        'heartbeat' => 45,
    ]);

    // --- Act ---
    $config = (new RabbitMqConnectionFactory)->configuration($connection);

    // --- Assert ---
    expect($config->getHost())->toBe('rabbit.internal');
    expect($config->getPort())->toBe(5_679);
    expect($config->getUser())->toBe('publisher@tenant');
    expect($config->getPassword())->toBe('p@ss');
    expect($config->getVhost())->toBe('orders/production');
    expect($config->getConnectionName())->toBe('spoolrail:events');
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
        'url' => 'amqp://guest:guest@rabbit.internal',
        'heartbeat' => 0,
    ]);

    // --- Act ---
    $config = (new RabbitMqConnectionFactory)->configuration($connection);

    // --- Assert ---
    expect($config->isSecure())->toBeFalse();
    expect($config->getSslVerify())->toBeNull();
    expect($config->getSslVerifyName())->toBeNull();
    expect($config->getSslCaCert())->toBeNull();
    expect($config->getVhost())->toBe('/');
    expect($config->getHeartbeat())->toBe(0);
    expect($config->isKeepalive())->toBeTrue();
    expect($config->getReadTimeout())->toBe(3.0);
    expect($config->getWriteTimeout())->toBe(3.0);
});

test('rejects a detected RabbitMQ version older than 4.3', function (): void {
    // --- Arrange ---
    $native = Mockery::mock(AbstractConnection::class);
    $native->shouldReceive('getServerProperties')
        ->once()
        ->andReturn(['version' => ['S', '4.2.9']]);
    $native->shouldReceive('close')->once();

    // --- Act & Assert ---
    expect(fn () => (new RabbitMqConnectionFactory)->verifySupportedBroker($native))
        ->toThrow(
            UnsupportedRabbitMqVersionException::class,
            'RabbitMQ [4.2.9] is not supported; Spoolrail requires RabbitMQ 4.3 or later.',
        );
});
