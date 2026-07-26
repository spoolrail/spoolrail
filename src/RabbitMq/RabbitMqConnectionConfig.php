<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Illuminate\Support\Arr;
use Spoolrail\Spoolrail\Exceptions\RabbitMqConfigurationException;

readonly class RabbitMqConnectionConfig
{
    /**
     * @param  array<array-key, mixed>  $configuration
     */
    public function __construct(
        public string $connection,
        private array $configuration,
    ) {
        $this->scheme();
        $this->hosts();
    }

    public function scheme(): string
    {
        $scheme = Arr::string($this->configuration, 'scheme', 'amqp');

        if (! in_array($scheme, ['amqp', 'amqps'], true)) {
            throw RabbitMqConfigurationException::invalid(
                $this->connection,
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
        if (array_key_exists('host', $this->configuration) && array_key_exists('hosts', $this->configuration)) {
            throw RabbitMqConfigurationException::invalid(
                $this->connection,
                'hosts',
                'cannot be configured together with [host]',
            );
        }

        $hosts = Arr::wrap($this->configuration['hosts'] ?? $this->configuration['host'] ?? '127.0.0.1');

        if ($hosts === []) {
            throw RabbitMqConfigurationException::invalid(
                $this->connection,
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
        return Arr::string($this->configuration, 'username', 'guest');
    }

    public function password(): string
    {
        return Arr::string($this->configuration, 'password', 'guest');
    }

    public function virtualHost(): string
    {
        return Arr::string($this->configuration, 'vhost', '/');
    }

    public function caFile(): ?string
    {
        return isset($this->configuration['ca_file'])
            ? Arr::string($this->configuration, 'ca_file')
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

    public function consumerAcknowledgementTimeoutMilliseconds(): ?int
    {
        $seconds = $this->configuration['consumer_ack_timeout'] ?? null;

        return $seconds === null || $seconds === ''
            ? null
            : $this->integer('consumer_ack_timeout', 0) * 1_000;
    }

    public function management(): RabbitMqManagementConfig
    {
        $configuration = Arr::array($this->configuration, 'management', []);

        return new RabbitMqManagementConfig(
            $this->managementUrl($configuration),
            Arr::string($configuration, 'username', $this->username()),
            Arr::string($configuration, 'password', $this->password()),
            isset($configuration['ca_file'])
                ? Arr::string($configuration, 'ca_file')
                : null,
        );
    }

    /**
     * @param  array<array-key, mixed>  $configuration
     */
    private function managementUrl(array $configuration): string
    {
        $url = Arr::string($configuration, 'url', 'http://127.0.0.1:15672');
        $parts = parse_url($url);

        if (
            ! is_array($parts)
            || ! isset($parts['scheme'], $parts['host'])
            || ! in_array($parts['scheme'], ['http', 'https'], true)
            || isset($parts['user'])
            || isset($parts['pass'])
            || isset($parts['query'])
            || isset($parts['fragment'])
        ) {
            throw RabbitMqConfigurationException::invalid(
                $this->connection,
                'management.url',
                'must be an HTTP or HTTPS endpoint without embedded credentials, query, or fragment',
            );
        }

        return rtrim($url, '/');
    }

    private function integer(string $key, int $default): int
    {
        /** @var int|numeric-string $value */
        $value = $this->configuration[$key] ?? $default;

        return (int) $value;
    }
}
