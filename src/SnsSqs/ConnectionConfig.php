<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\SnsSqs;

use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;

readonly class ConnectionConfig
{
    /**
     * @param  array<array-key, mixed>  $config
     */
    public function __construct(
        public string $connectionName,
        private array $config,
    ) {
        $this->region();
        $this->accountId();
        $this->endpoint();
        $this->fifo();
        $this->credentials();
        $this->connectionTimeout();
        $this->requestTimeout();
    }

    public function region(): string
    {
        return $this->requiredString('region', 'must be a non-empty string');
    }

    public function accountId(): string
    {
        $accountId = $this->requiredString('account_id', 'must be a 12-digit AWS account ID');

        if (preg_match('/\A\d{12}\z/', $accountId) !== 1) {
            $this->reject('account_id', 'must be a 12-digit AWS account ID');
        }

        return $accountId;
    }

    public function endpoint(): ?string
    {
        $endpoint = $this->optionalString('endpoint');

        if ($endpoint === null) {
            return null;
        }

        $parts = parse_url($endpoint);

        if (! is_array($parts)) {
            $this->rejectEndpoint();
        }

        if (! $this->isSupportedEndpoint($parts)) {
            $this->rejectEndpoint();
        }

        return rtrim($endpoint, '/');
    }

    public function fifo(): bool
    {
        $fifo = $this->config['fifo'] ?? true;

        if (! is_bool($fifo)) {
            $this->reject('fifo', 'must be a boolean');
        }

        return $fifo;
    }

    public function connectionTimeout(): int
    {
        return $this->positiveInteger('connection_timeout', 3);
    }

    public function requestTimeout(): int
    {
        return $this->positiveInteger('request_timeout', 60);
    }

    /**
     * @return array{key: string, secret: string, token?: string}|null
     */
    public function credentials(): ?array
    {
        $credentials = array_filter([
            'key' => $this->optionalString('key'),
            'secret' => $this->optionalString('secret'),
            'token' => $this->optionalString('token'),
        ], static fn (?string $value): bool => $value !== null);

        if ($credentials === []) {
            return null;
        }

        if (! isset($credentials['key'], $credentials['secret'])) {
            $this->reject(
                'credentials',
                'must provide both [key] and [secret] before an optional [token], or leave all three absent',
            );
        }

        return $credentials;
    }

    /**
     * @return array<string, mixed>
     */
    public function clientOptions(): array
    {
        $options = [
            'version' => 'latest',
            'region' => $this->region(),
            'http' => [
                'connect_timeout' => $this->connectionTimeout(),
                'timeout' => $this->requestTimeout(),
            ],
        ];

        if (($endpoint = $this->endpoint()) !== null) {
            $options['endpoint'] = $endpoint;
        }

        if (($credentials = $this->credentials()) !== null) {
            $options['credentials'] = $credentials;
        }

        return $options;
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

    /**
     * @param  array<string, mixed>  $parts
     */
    private function isSupportedEndpoint(array $parts): bool
    {
        return in_array($parts['scheme'] ?? null, ['http', 'https'], true)
            && isset($parts['host'])
            && array_intersect_key(
                $parts,
                array_flip(['user', 'pass', 'query', 'fragment']),
            ) === [];
    }

    private function rejectEndpoint(): never
    {
        $this->reject(
            'endpoint',
            'must be an HTTP or HTTPS endpoint without embedded credentials, query, or fragment',
        );
    }

    private function positiveInteger(string $key, int $default): int
    {
        $value = $this->config[$key] ?? $default;

        if (! is_int($value) || $value < 1) {
            $this->reject($key, 'must be a positive integer');
        }

        return $value;
    }

    private function reject(string $setting, string $requirement): never
    {
        throw InvalidConfigException::snsSqsSetting(
            $this->connectionName,
            $setting,
            $requirement,
        );
    }
}
