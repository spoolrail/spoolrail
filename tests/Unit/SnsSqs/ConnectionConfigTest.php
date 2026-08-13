<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\SnsSqs\ConnectionConfig;

test('uses provider-chain credentials and bounded request defaults', function (): void {
    $config = new ConnectionConfig('snssqs', [
        'region' => 'us-east-1',
        'account_id' => '123456789012',
    ]);

    expect($config->fifo())->toBeTrue();
    expect($config->connectionTimeout())->toBe(3);
    expect($config->requestTimeout())->toBe(60);
    expect($config->credentials())->toBeNull();
    expect($config->clientOptions())->not->toHaveKey('credentials');
});

test('retains SDK retries only for ordinary clients', function (): void {
    $config = new ConnectionConfig('snssqs', [
        'region' => 'us-east-1',
        'account_id' => '123456789012',
    ]);

    expect($config->clientOptions())->not->toHaveKey('retries');
    expect($config->singleAttemptClientOptions()['retries'])->toBe(0);
});

test('passes a complete static credential set and custom endpoint to AWS clients', function (): void {
    $config = new ConnectionConfig('snssqs', [
        'key' => 'access-key',
        'secret' => 'secret-key',
        'token' => 'session-token',
        'region' => 'eu-west-1',
        'account_id' => '123456789012',
        'endpoint' => 'http://127.0.0.1:4566/',
        'fifo' => false,
        'connection_timeout' => 7,
        'request_timeout' => 29,
    ]);

    $options = $config->clientOptions();

    expect($config->fifo())->toBeFalse();
    expect($config->endpoint())->toBe('http://127.0.0.1:4566');
    expect($options['credentials'])->toBe([
        'key' => 'access-key',
        'secret' => 'secret-key',
        'token' => 'session-token',
    ]);
    expect($options['http'])->toBe([
        'connect_timeout' => 7,
        'timeout' => 29,
    ]);
});

test('rejects partial static credentials', function (array $credentials): void {
    expect(fn (): ConnectionConfig => new ConnectionConfig('snssqs', [
        ...$credentials,
        'region' => 'us-east-1',
        'account_id' => '123456789012',
    ]))->toThrow(
        InvalidConfigException::class,
        'must provide both [key] and [secret]',
    );
})->with([
    'key only' => [['key' => 'access-key']],
    'secret only' => [['secret' => 'secret-key']],
    'token only' => [['token' => 'session-token']],
    'key and token' => [['key' => 'access-key', 'token' => 'session-token']],
]);

test('rejects invalid owner and timeout settings', function (array $changes, string $message): void {
    expect(fn (): ConnectionConfig => new ConnectionConfig('snssqs', [
        'region' => 'us-east-1',
        'account_id' => '123456789012',
        ...$changes,
    ]))->toThrow(InvalidConfigException::class, $message);
})->with([
    'account ID' => [['account_id' => '1234'], 'must be a 12-digit AWS account ID'],
    'connection timeout' => [['connection_timeout' => 0], 'must be a positive integer'],
    'request timeout' => [['request_timeout' => -1], 'must be a positive integer'],
    'FIFO mode' => [['fifo' => 'true'], 'must be a boolean'],
]);
