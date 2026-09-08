<?php

declare(strict_types=1);

use Google\Auth\Credentials\ServiceAccountCredentials;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\V1\Client\SubscriberClient;
use Google\Cloud\PubSub\V1\PullRequest;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Response;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig;

beforeEach(function (): void {
    $previous = getenv('PUBSUB_EMULATOR_HOST');
    putenv('PUBSUB_EMULATOR_HOST');

    $this->beforeApplicationDestroyed(static function () use ($previous): void {
        $previous === false ? putenv('PUBSUB_EMULATOR_HOST') : putenv("PUBSUB_EMULATOR_HOST=$previous");
    });
});

test('leaves authentication to ADC when credentials are not configured', function (): void {
    $config = new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail-production',
    ]);
    $options = $config->clientOptions();

    expect($options)->toMatchArray([
        'projectId' => 'spoolrail-production',
        'transport' => 'rest',
    ]);
    expect($options)->not->toHaveKey('credentials');
});

test('defaults the Pub/Sub receive batch size to ten', function (): void {
    $config = new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail-production',
    ]);

    expect($config->receiveBatchSize())->toBe(10);
});

test('accepts one thousand as the Pub/Sub receive batch size', function (): void {
    $config = new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail-production',
        'receive_batch_size' => 1_000,
    ]);

    expect($config->receiveBatchSize())->toBe(1_000);
});

test('defaults the Pub/Sub acknowledgment deadline to thirty seconds', function (): void {
    $config = new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail-production',
    ]);

    expect($config->acknowledgmentDeadline())->toBe(30);
});

test('accepts ten minutes as the Pub/Sub acknowledgment deadline', function (): void {
    $config = new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail-production',
        'acknowledgment_deadline' => 600,
    ]);

    expect($config->acknowledgmentDeadline())->toBe(600);
});

test('supplies configured service-account credentials to clients', function (): void {
    // --- Arrange ---
    $path = temporaryPubSubCredential([
        'type' => 'service_account',
        'project_id' => 'credential-project',
        'client_email' => 'spoolrail@credential-project.iam.gserviceaccount.com',
        'private_key' => "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n",
    ]);

    try {
        // --- Act ---
        $config = new ConnectionConfig('pubsub', [
            'project_id' => 'resource-project',
            'credentials' => $path,
        ]);

        // --- Assert ---
        expect($config->clientOptions()['credentials'])
            ->toBeInstanceOf(ServiceAccountCredentials::class);
    } finally {
        unlink($path);
    }
});

test('routes publishing and consumption through the CI emulator without authentication', function (): void {
    // --- Arrange ---
    putenv('PUBSUB_EMULATOR_HOST=pubsub:8085');
    $publisherHandler = new MockHandler([new Response(200, [], '{"messageIds":["local-message"]}')]);
    $subscriberHandler = new MockHandler([new Response(200, [], '{}')]);

    $config = new ConnectionConfig('pubsub', ['project_id' => 'warehouse']);
    $publisher = new PubSubClient([
        ...$config->singleAttemptClientOptions(),
        'transportConfig' => ['rest' => ['httpHandler' => $publisherHandler]],
    ]);
    $subscriber = new SubscriberClient($config->subscriberClientOptions($subscriberHandler));

    // --- Act ---
    $publisher->topic('orders')->publish(['data' => 'order-created']);
    $subscriber->pull(new PullRequest([
        'subscription' => 'projects/warehouse/subscriptions/orders',
        'max_messages' => 1,
    ]));

    // --- Assert ---
    expect((string) $publisherHandler->getLastRequest()?->getUri()->withQuery(''))
        ->toBe('http://pubsub:8085/v1/projects/warehouse/topics/orders:publish');
    expect((string) $subscriberHandler->getLastRequest()?->getUri()->withQuery(''))
        ->toBe('http://pubsub:8085/v1/projects/warehouse/subscriptions/orders:pull');
    expect($publisherHandler->getLastRequest()?->hasHeader('Authorization'))->toBeFalse();
    expect($subscriberHandler->getLastRequest()?->hasHeader('Authorization'))->toBeFalse();
});

test('preserves authenticated HTTPS requests for production clients', function (): void {
    // --- Arrange ---
    putenv('PUBSUB_EMULATOR_HOST=');
    openssl_pkey_export(openssl_pkey_new(['private_key_bits' => 2048]), $privateKey);
    $path = temporaryPubSubCredential([
        'type' => 'service_account',
        'client_email' => 'worker@warehouse.iam.gserviceaccount.com',
        'private_key' => $privateKey,
    ]);
    $publisherHandler = new MockHandler([new Response(200, [], '{"messageIds":["production-message"]}')]);
    $subscriberHandler = new MockHandler([new Response(200, [], '{}')]);
    $authOptions = ['authHttpHandler' => static fn (): Response => new Response(200, [], '{"access_token":"test-token","expires_in":3600,"token_type":"Bearer"}')];

    try {
        $config = new ConnectionConfig('production', [
            'project_id' => 'warehouse',
            'endpoint' => 'europe-west1-pubsub.googleapis.com:443',
            'credentials' => $path,
        ]);
        $publisher = new PubSubClient([
            ...$config->singleAttemptClientOptions(),
            'credentialsConfig' => $authOptions,
            'transportConfig' => ['rest' => ['httpHandler' => $publisherHandler]],
        ]);
        $subscriber = new SubscriberClient([
            ...$config->subscriberClientOptions($subscriberHandler),
            'credentialsConfig' => $authOptions,
        ]);

        // --- Act ---
        $publisher->topic('orders')->publish(['data' => 'order-created']);
        $subscriber->pull(new PullRequest([
            'subscription' => 'projects/warehouse/subscriptions/orders',
            'max_messages' => 1,
        ]));

        // --- Assert ---
        expect((string) $publisherHandler->getLastRequest()?->getUri()->withQuery(''))
            ->toBe('https://europe-west1-pubsub.googleapis.com/v1/projects/warehouse/topics/orders:publish');
        expect((string) $subscriberHandler->getLastRequest()?->getUri()->withQuery(''))
            ->toBe('https://europe-west1-pubsub.googleapis.com/v1/projects/warehouse/subscriptions/orders:pull');
        expect($publisherHandler->getLastRequest()?->getHeaderLine('Authorization'))->toStartWith('Bearer ');
        expect($subscriberHandler->getLastRequest()?->getHeaderLine('Authorization'))->toStartWith('Bearer ');
    } finally {
        unlink($path);
    }
});

test('rejects emulator routing alongside an explicit production endpoint', function (): void {
    // --- Arrange ---
    putenv('PUBSUB_EMULATOR_HOST=pubsub:8085');

    // --- Act & Assert ---
    expect(fn (): ConnectionConfig => new ConnectionConfig('production', [
        'project_id' => 'warehouse-production',
        'endpoint' => 'europe-west1-pubsub.googleapis.com:443',
    ]))->toThrow(InvalidConfigException::class, '[endpoint] cannot be configured together with PUBSUB_EMULATOR_HOST');
});

test('rejects emulator routing before loading explicit credentials', function (): void {
    // --- Arrange ---
    putenv('PUBSUB_EMULATOR_HOST=pubsub:8085');

    // --- Act & Assert ---
    expect(fn (): ConnectionConfig => new ConnectionConfig('production', [
        'project_id' => 'warehouse-production',
        'credentials' => '/not-mounted/production-credentials.json',
    ]))->toThrow(InvalidConfigException::class, '[credentials] cannot be configured together with PUBSUB_EMULATOR_HOST');
});

test('rejects emulator conflicts introduced after configuration construction', function (): void {
    // --- Arrange ---
    $config = new ConnectionConfig('production', [
        'project_id' => 'warehouse',
        'endpoint' => 'europe-west1-pubsub.googleapis.com:443',
    ]);
    putenv('PUBSUB_EMULATOR_HOST=pubsub:8085');

    // --- Act & Assert ---
    expect(fn (): array => $config->singleAttemptClientOptions())
        ->toThrow(InvalidConfigException::class, '[endpoint] cannot be configured together with PUBSUB_EMULATOR_HOST');
    expect(fn (): array => $config->subscriberClientOptions(new MockHandler))
        ->toThrow(InvalidConfigException::class, '[endpoint] cannot be configured together with PUBSUB_EMULATOR_HOST');
});

test('rejects a malformed emulator address instead of falling back to production', function (string $host): void {
    // --- Arrange ---
    putenv("PUBSUB_EMULATOR_HOST=$host");

    // --- Act & Assert ---
    expect(fn (): ConnectionConfig => new ConnectionConfig('pubsub', ['project_id' => 'warehouse']))
        ->toThrow(InvalidConfigException::class, '[PUBSUB_EMULATOR_HOST]');
})->with([' ', '0', 'http://pubsub:8085', 'pubsub:8085/path', 'pubsub:0', 'pubsub:65536', '[::1]:8085']);

test('disables SDK retries only for single-attempt clients', function (): void {
    $config = new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail-production',
    ]);

    expect($config->clientOptions())->not->toHaveKey('disableRetries');
    expect($config->singleAttemptClientOptions()['disableRetries'])->toBeTrue();
});

test('rejects an explicit credential instead of falling back to ADC', function (string $contents, string $message): void {
    // --- Arrange ---
    $path = tempnam(sys_get_temp_dir(), 'spoolrail-pubsub-');
    file_put_contents($path, $contents);

    try {
        // --- Act & Assert ---
        expect(fn (): ConnectionConfig => new ConnectionConfig('pubsub', [
            'project_id' => 'spoolrail-production',
            'credentials' => $path,
        ]))->toThrow(InvalidConfigException::class, $message);
    } finally {
        unlink($path);
    }
})->with([
    'invalid JSON' => ['{', 'must contain valid JSON'],
    'different credential type' => [json_encode(['type' => 'external_account'], JSON_THROW_ON_ERROR), 'must contain a service-account credential'],
    'incomplete service account' => [json_encode(['type' => 'service_account'], JSON_THROW_ON_ERROR), 'must contain a valid service-account credential'],
]);

test('rejects invalid connection settings', function (array $changes, string $message): void {
    expect(fn (): ConnectionConfig => new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail-production',
        ...$changes,
    ]))->toThrow(InvalidConfigException::class, $message);
})->with([
    'project ID' => [['project_id' => ''], 'must be a non-empty string'],
    'endpoint URL' => [['endpoint' => 'https://pubsub.googleapis.com'], 'must be a hostname with an optional port'],
    'endpoint port' => [['endpoint' => 'pubsub.googleapis.com:65536'], 'must be a hostname with an optional port'],
    'message ordering' => [['message_ordering' => 'true'], 'must be a boolean'],
    'exactly once' => [['exactly_once' => 1], 'must be a boolean'],
    'non-integer receive batch size' => [['receive_batch_size' => '1'], 'must be an integer from 1 through 1000'],
    'receive batch size below minimum' => [['receive_batch_size' => 0], 'must be an integer from 1 through 1000'],
    'receive batch size above maximum' => [['receive_batch_size' => 1_001], 'must be an integer from 1 through 1000'],
    'non-integer acknowledgment deadline' => [['acknowledgment_deadline' => '30'], 'must be an integer from 10 through 600'],
    'acknowledgment deadline below minimum' => [['acknowledgment_deadline' => 9], 'must be an integer from 10 through 600'],
    'acknowledgment deadline above maximum' => [['acknowledgment_deadline' => 601], 'must be an integer from 10 through 600'],
    'missing credential file' => [['credentials' => '/missing/spoolrail-pubsub.json'], 'must identify a readable JSON file'],
]);

/**
 * @param  array<string, string>  $credential
 */
function temporaryPubSubCredential(array $credential): string
{
    $path = tempnam(sys_get_temp_dir(), 'spoolrail-pubsub-');
    file_put_contents($path, json_encode($credential, JSON_THROW_ON_ERROR));

    return $path;
}
