<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\RabbitMqConfigurationException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqManagementConfig;

test('interprets the complete RabbitMQ connection configuration', function (): void {
    $config = new RabbitMqConnectionConfig('events', [
        'scheme' => 'amqps',
        'host' => 'rabbit.internal',
        'port' => '5679',
        'username' => 'publisher@tenant',
        'password' => 'p@ss',
        'vhost' => 'orders/production',
        'ca_file' => __FILE__,
        'connection_timeout' => '7',
        'heartbeat' => '0',
        'publisher_confirm_timeout' => '17',
        'prefetch' => '65535',
        'consumer_ack_timeout' => '30',
        'management' => [
            'url' => 'https://rabbit.internal:15671/api/',
            'username' => 'topology',
            'password' => 'secret',
            'ca_file' => __DIR__.'/../OwnershipPrefixTest.php',
        ],
    ]);

    $management = $config->management();

    expect($config->scheme())->toBe('amqps');
    expect($config->hosts())->toBe(['rabbit.internal']);
    expect($config->port())->toBe(5_679);
    expect($config->username())->toBe('publisher@tenant');
    expect($config->password())->toBe('p@ss');
    expect($config->virtualHost())->toBe('orders/production');
    expect($config->caFile())->toBe(__FILE__);
    expect($config->connectionTimeout())->toBe(7);
    expect($config->heartbeat())->toBe(0);
    expect($config->publisherConfirmTimeout())->toBe(17);
    expect($config->prefetch())->toBe(65_535);
    expect($config->consumerAcknowledgementTimeoutMilliseconds())->toBe(30_000);
    expect($management->url)->toBe('https://rabbit.internal:15671/api');
    expect($management->username)->toBe('topology');
    expect($management->password)->toBe('secret');
    expect($management->caFile)->toBe(__DIR__.'/../OwnershipPrefixTest.php');
});

test('applies RabbitMQ local defaults and keeps management TLS trust independent', function (): void {
    $config = new RabbitMqConnectionConfig('events', [
        'ca_file' => __FILE__,
    ]);

    $management = $config->management();

    expect($config->scheme())->toBe('amqp');
    expect($config->hosts())->toBe(['127.0.0.1']);
    expect($config->port())->toBe(5_672);
    expect($config->username())->toBe('guest');
    expect($config->password())->toBe('guest');
    expect($config->virtualHost())->toBe('/');
    expect($config->connectionTimeout())->toBe(3);
    expect($config->heartbeat())->toBe(60);
    expect($config->publisherConfirmTimeout())->toBe(60);
    expect($config->prefetch())->toBe(10);
    expect($config->consumerAcknowledgementTimeoutMilliseconds())->toBeNull();
    expect($management->url)->toBe('http://127.0.0.1:15672');
    expect($management->username)->toBe('guest');
    expect($management->password)->toBe('guest');
    expect($management->caFile)->toBeNull();
});

test('preserves the configured order of multiple RabbitMQ hosts', function (): void {
    $config = new RabbitMqConnectionConfig('events', [
        'hosts' => [
            'rabbit-a.internal',
            'rabbit-b.internal',
        ],
    ]);

    expect($config->hosts())->toBe([
        'rabbit-a.internal',
        'rabbit-b.internal',
    ]);
});

test('rejects conflicting host forms, empty host lists, and unsupported schemes', function (array $configuration, string $setting): void {
    expect(fn (): RabbitMqConnectionConfig => new RabbitMqConnectionConfig('events', $configuration))
        ->toThrow(function (RabbitMqConfigurationException $exception) use ($setting): void {
            expect($exception->getMessage())->toContain("setting [$setting]");
        });
})->with([
    'host and hosts together' => [[
        'host' => 'rabbit-a.internal',
        'hosts' => ['rabbit-b.internal'],
    ], 'hosts'],
    'empty hosts list' => [['hosts' => []], 'hosts'],
    'unsupported scheme' => [['scheme' => 'https'], 'scheme'],
]);

test('allows one management credential to override its corresponding AMQP credential', function (): void {
    $config = new RabbitMqConnectionConfig('events', [
        'username' => 'publisher',
        'password' => 'publisher-secret',
        'management' => [
            'username' => 'topology',
        ],
    ]);

    expect($config->management())
        ->username->toBe('topology')
        ->password->toBe('publisher-secret');
});

test('rejects management URLs with credentials, queries, or fragments', function (array $management, string $setting): void {
    $config = new RabbitMqConnectionConfig('events', [
        'management' => $management,
    ]);

    expect(fn (): RabbitMqManagementConfig => $config->management())
        ->toThrow(function (RabbitMqConfigurationException $exception) use ($setting): void {
            expect($exception->getMessage())->toContain("setting [$setting]");
        });
})->with([
    'embedded credentials' => [[
        'url' => 'https://admin:secret@rabbit.internal:15671/api',
    ], 'management.url'],
    'query' => [[
        'url' => 'https://rabbit.internal:15671/api?tenant=warehouse',
    ], 'management.url'],
    'fragment' => [[
        'url' => 'https://rabbit.internal:15671/api#topology',
    ], 'management.url'],
]);

test('never includes configured credentials in configuration diagnostics', function (): void {
    $management = new RabbitMqConnectionConfig('events', [
        'management' => [
            'url' => 'https://topology:management-secret@rabbit.internal:15671/api',
        ],
    ]);

    expect(fn (): RabbitMqManagementConfig => $management->management())
        ->toThrow(function (RabbitMqConfigurationException $exception): void {
            expect($exception->getMessage())
                ->not->toContain('topology')
                ->not->toContain('management-secret');
        });
});
