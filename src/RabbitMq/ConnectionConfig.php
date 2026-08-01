<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Illuminate\Support\Arr;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;

readonly class ConnectionConfig
{
    private const array FORBIDDEN_MANAGEMENT_URL_PARTS = [
        'user' => true,
        'pass' => true,
        'query' => true,
        'fragment' => true,
    ];

    /**
     * @param  array<array-key, mixed>  $config
     */
    public function __construct(
        public string $connectionName,
        private array $config,
    ) {
        $this->scheme();
        $this->hosts();
    }

    public function scheme(): string
    {
        $scheme = Arr::string($this->config, 'scheme', 'amqp');

        if (! in_array($scheme, ['amqp', 'amqps'], true)) {
            throw InvalidConfigException::rabbitMqSetting(
                $this->connectionName,
                'scheme',
                'must be [amqp] or [amqps]',
            );
        }

        return $scheme;
    }

    /**
     * @return non-empty-list<string>
     */
    public function hosts(): array
    {
        if (array_key_exists('host', $this->config) && array_key_exists('hosts', $this->config)) {
            throw InvalidConfigException::rabbitMqSetting(
                $this->connectionName,
                'hosts',
                'cannot be configured together with [host]',
            );
        }

        $hosts = Arr::wrap($this->config['hosts'] ?? $this->config['host'] ?? '127.0.0.1');

        if ($hosts === []) {
            throw InvalidConfigException::rabbitMqSetting(
                $this->connectionName,
                'hosts',
                'must be a non-empty list of hostnames',
            );
        }

        return array_map(
            fn (int|string $key): string => Arr::string($hosts, $key),
            array_keys($hosts),
        );
    }

    public function port(): int
    {
        return $this->integer('port', 5_672);
    }

    public function username(): string
    {
        return Arr::string($this->config, 'username', 'guest');
    }

    public function password(): string
    {
        return Arr::string($this->config, 'password', 'guest');
    }

    public function virtualHost(): string
    {
        return Arr::string($this->config, 'vhost', '/');
    }

    public function caFile(): ?string
    {
        return isset($this->config['ca_file'])
            ? Arr::string($this->config, 'ca_file')
            : null;
    }

    public function connectionTimeout(): int
    {
        return $this->integer('connection_timeout', 3);
    }

    public function heartbeat(): int
    {
        return $this->integer('heartbeat', 60);
    }

    public function publisherConfirmTimeout(): int
    {
        return $this->integer('publisher_confirm_timeout', 60);
    }

    public function prefetch(): int
    {
        return $this->integer('prefetch', 10);
    }

    public function management(): ManagementConfig
    {
        $config = Arr::array($this->config, 'management', []);

        return new ManagementConfig(
            $this->managementUrl($config),
            Arr::string($config, 'username', $this->username()),
            Arr::string($config, 'password', $this->password()),
            isset($config['ca_file'])
                ? Arr::string($config, 'ca_file')
                : null,
        );
    }

    /**
     * @param  array<array-key, mixed>  $config
     */
    private function managementUrl(array $config): string
    {
        $url = Arr::string($config, 'url', 'http://127.0.0.1:15672');
        $parts = (array) parse_url($url);

        if (! in_array($parts['scheme'] ?? null, ['http', 'https'], true)) {
            $this->rejectManagementUrl();
        }

        if (! isset($parts['host'])) {
            $this->rejectManagementUrl();
        }

        if (array_intersect_key($parts, self::FORBIDDEN_MANAGEMENT_URL_PARTS) !== []) {
            $this->rejectManagementUrl();
        }

        return rtrim($url, '/');
    }

    private function rejectManagementUrl(): never
    {
        throw InvalidConfigException::rabbitMqSetting(
            $this->connectionName,
            'management.url',
            'must be an HTTP or HTTPS endpoint without embedded credentials, query, or fragment',
        );
    }

    private function integer(string $key, int $default): int
    {
        /** @var int|numeric-string $value */
        $value = $this->config[$key] ?? $default;

        return (int) $value;
    }
}
