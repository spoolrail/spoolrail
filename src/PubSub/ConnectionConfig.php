<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\PubSub;

use Google\ApiCore\InsecureCredentialsWrapper;
use Google\Auth\Credentials\ServiceAccountCredentials;
use JsonException;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Throwable;

readonly class ConnectionConfig
{
    private const string PUBSUB_SCOPE = 'https://www.googleapis.com/auth/pubsub';

    private ?ServiceAccountCredentials $credentials;

    /**
     * @param  array<array-key, mixed>  $config
     */
    public function __construct(
        public string $connectionName,
        private array $config,
    ) {
        $this->projectId();
        $this->endpoint();
        $this->messageOrdering();
        $this->exactlyOnce();
        $this->receiveBatchSize();
        $this->acknowledgmentDeadline();
        $this->credentials = $this->resolveCredentials();
    }

    public function projectId(): string
    {
        return $this->requiredString('project_id', 'must be a non-empty string');
    }

    public function endpoint(): ?string
    {
        $endpoint = $this->optionalString('endpoint');

        if ($endpoint === null) {
            return null;
        }

        if (
            preg_match('/\A[A-Za-z0-9.-]+(?::\d{1,5})?\z/', $endpoint) !== 1
            || parse_url("https://$endpoint") === false
        ) {
            $this->rejectEndpoint();
        }

        return $endpoint;
    }

    public function messageOrdering(): bool
    {
        return $this->boolean('message_ordering', true);
    }

    public function exactlyOnce(): bool
    {
        return $this->boolean('exactly_once', true);
    }

    public function receiveBatchSize(): int
    {
        $value = $this->config['receive_batch_size'] ?? 10;

        if (! is_int($value) || $value < 1 || $value > 1_000) {
            $this->reject('receive_batch_size', 'must be an integer from 1 through 1000');
        }

        return $value;
    }

    public function acknowledgmentDeadline(): int
    {
        $value = $this->config['acknowledgment_deadline'] ?? 30;

        if (! is_int($value) || $value < 10 || $value > 600) {
            $this->reject('acknowledgment_deadline', 'must be an integer from 10 through 600');
        }

        return $value;
    }

    /**
     * @return array<string, mixed>
     */
    public function clientOptions(): array
    {
        $options = [
            'projectId' => $this->projectId(),
            'transport' => 'rest',
        ];

        if (($endpoint = $this->endpoint()) !== null) {
            $options['apiEndpoint'] = $endpoint;
        }

        if ($this->credentials instanceof ServiceAccountCredentials) {
            $options['credentials'] = $this->credentials;
        } elseif ((bool) getenv('PUBSUB_EMULATOR_HOST')) {
            $options['credentials'] = new InsecureCredentialsWrapper;
        }

        return $options;
    }

    /**
     * @return array<string, mixed>
     */
    public function singleAttemptClientOptions(): array
    {
        return [
            ...$this->clientOptions(),
            'disableRetries' => true,
        ];
    }

    private function resolveCredentials(): ?ServiceAccountCredentials
    {
        $path = $this->optionalString('credentials');

        if ($path === null) {
            return null;
        }

        if (! is_file($path) || ! is_readable($path)) {
            $this->reject('credentials', 'must identify a readable JSON file');
        }

        $contents = file_get_contents($path);

        if (! is_string($contents)) {
            $this->reject('credentials', 'must identify a readable JSON file');
        }

        try {
            $credential = json_decode($contents, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            $this->reject('credentials', 'must contain valid JSON');
        }

        if (! is_array($credential) || ($credential['type'] ?? null) !== 'service_account') {
            $this->reject('credentials', 'must contain a service-account credential');
        }

        try {
            return new ServiceAccountCredentials(self::PUBSUB_SCOPE, $credential);
        } catch (Throwable) {
            $this->reject('credentials', 'must contain a valid service-account credential');
        }
    }

    private function requiredString(string $key, string $requirement): string
    {
        $value = $this->config[$key] ?? null;

        if (! is_string($value) || trim($value) === '') {
            $this->reject($key, $requirement);
        }

        return $value;
    }

    private function optionalString(string $key): ?string
    {
        $value = $this->config[$key] ?? null;

        if ($value === null || $value === '') {
            return null;
        }

        return $this->requiredString($key, 'must be null or a non-empty string');
    }

    private function boolean(string $key, bool $default): bool
    {
        $value = $this->config[$key] ?? $default;

        if (! is_bool($value)) {
            $this->reject($key, 'must be a boolean');
        }

        return $value;
    }

    private function rejectEndpoint(): never
    {
        $this->reject('endpoint', 'must be a hostname with an optional port');
    }

    private function reject(string $setting, string $requirement): never
    {
        throw InvalidConfigException::pubSubSetting(
            $this->connectionName,
            $setting,
            $requirement,
        );
    }
}
