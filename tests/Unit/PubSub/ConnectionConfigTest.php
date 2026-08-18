<?php

declare(strict_types=1);

use Google\Auth\Credentials\ServiceAccountCredentials;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig;

test('leaves authentication to ADC when credentials are not configured', function (): void {
    // --- Arrange ---
    $emulatorHost = getenv('PUBSUB_EMULATOR_HOST');
    putenv('PUBSUB_EMULATOR_HOST');

    try {
        // --- Act ---
        $config = new ConnectionConfig('pubsub', [
            'project_id' => 'spoolrail-production',
        ]);
        $options = $config->clientOptions();

        // --- Assert ---
        expect($options)->toMatchArray([
            'projectId' => 'spoolrail-production',
            'transport' => 'rest',
        ]);
        expect($options)->not->toHaveKey('credentials');
    } finally {
        $emulatorHost === false
            ? putenv('PUBSUB_EMULATOR_HOST')
            : putenv("PUBSUB_EMULATOR_HOST=$emulatorHost");
    }
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

test('routes clients through the configured Pub/Sub endpoint', function (): void {
    $config = new ConnectionConfig('pubsub', [
        'project_id' => 'spoolrail-production',
        'endpoint' => 'us-central1-pubsub.googleapis.com:443',
    ]);

    expect($config->clientOptions()['apiEndpoint'])
        ->toBe('us-central1-pubsub.googleapis.com:443');
});

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
