<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\RabbitMqConfigurationException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqManagementConfig;

test('interprets the complete RabbitMQ connection configuration', function (): void {
    // --- Arrange ---
    $config = new RabbitMqConnectionConfig('events', [
        'url' => 'amqps://publisher%40tenant:p%40ss@rabbit.internal/orders%2Fproduction',
        'ca_file' => __FILE__,
        'heartbeat' => 0,
        'publisher_confirm_timeout' => 17,
        'prefetch' => 65_535,
        'consumer_ack_timeout' => 30,
        'management' => [
            'url' => 'https://rabbit.internal:15671/api/',
            'username' => 'topology',
            'password' => 'secret',
            'ca_file' => __DIR__.'/OwnershipPrefixTest.php',
        ],
    ]);

    // --- Act ---
    $management = $config->management();

    // --- Assert ---
    expect($config->scheme())->toBe('amqps');
    expect($config->host())->toBe('rabbit.internal');
    expect($config->port())->toBe(5_671);
    expect($config->username())->toBe('publisher@tenant');
    expect($config->password())->toBe('p@ss');
    expect($config->virtualHost())->toBe('orders/production');
    expect($config->caFile())->toBe(__FILE__);
    expect($config->heartbeat())->toBe(0);
    expect($config->publisherConfirmTimeout())->toBe(17);
    expect($config->prefetch())->toBe(65_535);
    expect($config->consumerAcknowledgementTimeoutMilliseconds())->toBe(30_000);
    expect($management->url)->toBe('https://rabbit.internal:15671/api');
    expect($management->username)->toBe('topology');
    expect($management->password)->toBe('secret');
    expect($management->caFile)->toBe(__DIR__.'/OwnershipPrefixTest.php');
});

test('applies RabbitMQ connection defaults and reuses AMQP credentials for management', function (): void {
    // --- Arrange ---
    $config = new RabbitMqConnectionConfig('events', [
        'url' => 'amqp://publisher%40tenant:p%40ss@rabbit.internal/%2F',
        'management' => [
            'url' => 'http://rabbit.internal:15672/api',
        ],
    ]);

    // --- Act ---
    $management = $config->management();

    // --- Assert ---
    expect($config->port())->toBe(5_672);
    expect($config->virtualHost())->toBe('/');
    expect($config->heartbeat())->toBe(60);
    expect($config->publisherConfirmTimeout())->toBe(60);
    expect($config->prefetch())->toBe(10);
    expect($config->consumerAcknowledgementTimeoutMilliseconds())->toBeNull();
    expect($management->username)->toBe('publisher@tenant');
    expect($management->password)->toBe('p@ss');
});

test('maps supported RabbitMQ URI virtual-host semantics', function (string $uri): void {
    // --- Act ---
    $config = new RabbitMqConnectionConfig('events', ['url' => $uri]);

    // --- Assert ---
    expect($config->virtualHost())->toBe('/');
})->with([
    'absent virtual host defaults to root' => ['amqp://guest:guest@rabbit.internal'],
    'encoded root virtual host' => ['amqp://guest:guest@rabbit.internal/%2F'],
]);

test('rejects the explicit empty virtual host with a supported root-vhost remedy', function (): void {
    // --- Act & Assert ---
    expect(fn (): RabbitMqConnectionConfig => new RabbitMqConnectionConfig('events', [
        'url' => 'amqp://guest:guest@rabbit.internal/',
    ]))->toThrow(
        RabbitMqConfigurationException::class,
        'selects the empty virtual host, which php-amqplib does not support; omit the trailing slash or use [/%2F] for the root virtual host',
    );
});

test('defers management configuration until it is requested', function (): void {
    // --- Act ---
    $config = new RabbitMqConnectionConfig('events', [
        'url' => 'amqp://guest:guest@rabbit.internal',
    ]);

    // --- Assert ---
    expect($config->host())->toBe('rabbit.internal');
    expect(fn (): RabbitMqManagementConfig => $config->management())
        ->toThrow(RabbitMqConfigurationException::class, 'must configure [management.url]');
});

test('rejects invalid RabbitMQ data-plane configuration', function (array $configuration, string $setting): void {
    // --- Act ---
    $failure = null;

    try {
        new RabbitMqConnectionConfig('events', [
            'url' => 'amqp://guest:guest@rabbit.internal/%2F',
            ...$configuration,
        ]);
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(RabbitMqConfigurationException::class);
    expect($failure?->getMessage())->toContain("setting [$setting]");
})->with([
    'unsupported URI scheme' => [['url' => 'https://guest:guest@rabbit.internal/%2F'], 'url'],
    'URI without credentials' => [['url' => 'amqp://rabbit.internal/%2F'], 'url'],
    'URI query' => [['url' => 'amqp://guest:guest@rabbit.internal/%2F?heartbeat=10'], 'url'],
    'unescaped virtual-host slash' => [['url' => 'amqp://guest:guest@rabbit.internal/orders/production'], 'url'],
    'malformed credential encoding' => [['url' => 'amqp://guest%ZZ:guest@rabbit.internal/%2F'], 'url'],
    'malformed virtual-host encoding' => [['url' => 'amqp://guest:guest@rabbit.internal/%ZZ'], 'url'],
    'heartbeat above AMQP short' => [['heartbeat' => 65_536], 'heartbeat'],
    'zero publisher confirmation timeout' => [['publisher_confirm_timeout' => 0], 'publisher_confirm_timeout'],
    'unlimited prefetch' => [['prefetch' => 0], 'prefetch'],
    'prefetch above AMQP short' => [['prefetch' => 65_536], 'prefetch'],
    'non-string CA bundle' => [['ca_file' => false], 'ca_file'],
    'zero acknowledgement timeout' => [['consumer_ack_timeout' => 0], 'consumer_ack_timeout'],
    'non-integer acknowledgement timeout' => [['consumer_ack_timeout' => '30'], 'consumer_ack_timeout'],
    'acknowledgement timeout above AMQP long after conversion' => [
        ['consumer_ack_timeout' => 2_147_484],
        'consumer_ack_timeout',
    ],
]);

test('requires complete management credentials when either override is configured', function (array $credentials): void {
    // --- Arrange ---
    $config = new RabbitMqConnectionConfig('events', [
        'url' => 'amqp://guest:guest@rabbit.internal/%2F',
        'management' => [
            'url' => 'http://rabbit.internal:15672/api',
            ...$credentials,
        ],
    ]);

    // --- Act ---
    $failure = null;

    try {
        $config->management();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(RabbitMqConfigurationException::class);
    expect($failure?->getMessage())->toContain(
        'must configure both [management.username] and [management.password]',
    );
})->with([
    'username only' => [['username' => 'topology']],
    'password only' => [['password' => 'secret']],
]);

test('rejects unsafe management configuration', function (array $management, string $setting): void {
    // --- Arrange ---
    $config = new RabbitMqConnectionConfig('events', [
        'url' => 'amqp://guest:guest@rabbit.internal/%2F',
        'management' => $management,
    ]);

    // --- Act ---
    $failure = null;

    try {
        $config->management();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    // --- Assert ---
    expect($failure)->toBeInstanceOf(RabbitMqConfigurationException::class);
    expect($failure?->getMessage())->toContain("setting [$setting]");
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
    'non-string CA bundle' => [[
        'url' => 'https://rabbit.internal:15671/api',
        'ca_file' => false,
    ], 'management.ca_file'],
]);

test('never includes configured credentials in configuration diagnostics', function (): void {
    // --- Act ---
    $dataPlaneFailure = null;

    try {
        new RabbitMqConnectionConfig('events', [
            'url' => 'amqp://publisher:amqp-secret@rabbit.internal/%2F?unsupported=true',
        ]);
    } catch (Throwable $exception) {
        $dataPlaneFailure = $exception;
    }

    $management = new RabbitMqConnectionConfig('events', [
        'url' => 'amqp://publisher:amqp-secret@rabbit.internal/%2F',
        'management' => [
            'url' => 'https://topology:management-secret@rabbit.internal:15671/api',
        ],
    ]);

    $managementFailure = null;

    try {
        $management->management();
    } catch (Throwable $exception) {
        $managementFailure = $exception;
    }

    // --- Assert ---
    expect($dataPlaneFailure)->toBeInstanceOf(RabbitMqConfigurationException::class);
    expect($dataPlaneFailure?->getMessage())->not->toContain('publisher');
    expect($dataPlaneFailure?->getMessage())->not->toContain('amqp-secret');
    expect($managementFailure)->toBeInstanceOf(RabbitMqConfigurationException::class);
    expect($managementFailure?->getMessage())->not->toContain('topology');
    expect($managementFailure?->getMessage())->not->toContain('management-secret');
});
