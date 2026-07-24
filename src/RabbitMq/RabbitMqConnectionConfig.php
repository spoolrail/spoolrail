<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Spoolrail\Spoolrail\Exceptions\RabbitMqConfigurationException;

class RabbitMqConnectionConfig
{
    private const int MAX_AMQP_SHORT = 65_535;

    private const int MAX_AMQP_LONG = 2_147_483_647;

    private readonly mixed $managementConfiguration;

    private readonly string $scheme;

    private readonly string $host;

    private readonly int $port;

    private readonly string $username;

    private readonly string $password;

    private readonly string $virtualHost;

    private readonly ?string $caFile;

    private readonly int $heartbeat;

    private readonly int $publisherConfirmTimeout;

    private readonly int $prefetch;

    private readonly ?int $consumerAcknowledgementTimeoutMilliseconds;

    /**
     * @param  array<mixed>  $configuration
     */
    public function __construct(
        public readonly string $connection,
        array $configuration,
    ) {
        $uri = $this->parseUri($configuration);

        $this->managementConfiguration = $configuration['management'] ?? [];
        $this->scheme = $uri['scheme'];
        $this->host = $uri['host'];
        $this->port = $uri['port'];
        $this->username = $uri['username'];
        $this->password = $uri['password'];
        $this->virtualHost = $uri['virtual_host'];
        $this->caFile = $this->optionalReadableFile('ca_file', $configuration);
        $this->heartbeat = $this->integer($configuration, 'heartbeat', 60, 0, self::MAX_AMQP_SHORT);
        $this->publisherConfirmTimeout = $this->integer($configuration, 'publisher_confirm_timeout', 60, 1);
        $this->prefetch = $this->integer($configuration, 'prefetch', 10, 1, self::MAX_AMQP_SHORT);
        $this->consumerAcknowledgementTimeoutMilliseconds = $this->parseConsumerAcknowledgementTimeout($configuration);
    }

    public function scheme(): string
    {
        return $this->scheme;
    }

    public function host(): string
    {
        return $this->host;
    }

    public function port(): int
    {
        return $this->port;
    }

    public function username(): string
    {
        return $this->username;
    }

    public function password(): string
    {
        return $this->password;
    }

    public function virtualHost(): string
    {
        return $this->virtualHost;
    }

    public function caFile(): ?string
    {
        return $this->caFile;
    }

    public function heartbeat(): int
    {
        return $this->heartbeat;
    }

    public function publisherConfirmTimeout(): int
    {
        return $this->publisherConfirmTimeout;
    }

    public function prefetch(): int
    {
        return $this->prefetch;
    }

    public function consumerAcknowledgementTimeoutMilliseconds(): ?int
    {
        return $this->consumerAcknowledgementTimeoutMilliseconds;
    }

    public function management(): RabbitMqManagementConfig
    {
        $configuration = $this->managementConfiguration;

        if (! is_array($configuration)) {
            throw RabbitMqConfigurationException::invalid(
                $this->connection,
                'management',
                'must be an array',
            );
        }

        $url = $configuration['url'] ?? null;

        if (! is_string($url) || trim($url) === '') {
            throw RabbitMqConfigurationException::missingManagementUrl($this->connection);
        }

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

        $username = $configuration['username'] ?? null;
        $password = $configuration['password'] ?? null;

        if (($username === null) !== ($password === null)) {
            throw RabbitMqConfigurationException::incompleteManagementCredentials($this->connection);
        }

        if ($username !== null && (! is_string($username) || ! is_string($password))) {
            throw RabbitMqConfigurationException::incompleteManagementCredentials($this->connection);
        }

        if ($username === null) {
            $username = $this->username();
            $password = $this->password();
        }

        return new RabbitMqManagementConfig(
            rtrim($url, '/'),
            $username,
            $password,
            $this->optionalReadableFile('ca_file', $configuration, 'management.ca_file'),
        );
    }

    /**
     * @param  array<mixed>  $configuration
     * @return array{scheme: string, host: string, port: int, username: string, password: string, virtual_host: string}
     */
    private function parseUri(array $configuration): array
    {
        $url = $configuration['url'] ?? null;

        if (! is_string($url) || trim($url) === '') {
            throw RabbitMqConfigurationException::invalid($this->connection, 'url', 'must be a non-empty AMQP URI');
        }

        $parts = parse_url($url);

        if (
            ! is_array($parts)
            || ! isset($parts['scheme'], $parts['host'], $parts['user'], $parts['pass'])
            || ! in_array($parts['scheme'], ['amqp', 'amqps'], true)
            || isset($parts['query'])
            || isset($parts['fragment'])
        ) {
            throw RabbitMqConfigurationException::invalid(
                $this->connection,
                'url',
                'must be an amqp:// or amqps:// URI containing credentials and a host',
            );
        }

        $port = $parts['port'] ?? ($parts['scheme'] === 'amqps' ? 5_671 : 5_672);

        if ($port < 1) {
            throw RabbitMqConfigurationException::invalid($this->connection, 'url', 'contains an invalid port');
        }

        return [
            'scheme' => $parts['scheme'],
            'host' => $parts['host'],
            'port' => $port,
            'username' => $this->decodeUriComponent($parts['user']),
            'password' => $this->decodeUriComponent($parts['pass']),
            'virtual_host' => $this->parseVirtualHost($parts['path'] ?? null),
        ];
    }

    private function parseVirtualHost(?string $path): string
    {
        if ($path === null) {
            return '/';
        }

        if ($path === '/') {
            throw RabbitMqConfigurationException::invalid(
                $this->connection,
                'url',
                'selects the empty virtual host, which php-amqplib does not support; omit the trailing slash or use [/%2F] for the root virtual host',
            );
        }

        if (! str_starts_with($path, '/') || str_contains(substr($path, 1), '/')) {
            throw RabbitMqConfigurationException::invalid($this->connection, 'url', 'contains an invalid virtual host');
        }

        return $this->decodeUriComponent(substr($path, 1));
    }

    private function decodeUriComponent(string $value): string
    {
        if (preg_match('/%(?![A-Fa-f0-9]{2})/', $value) === 1) {
            throw RabbitMqConfigurationException::invalid($this->connection, 'url', 'contains invalid percent encoding');
        }

        return rawurldecode($value);
    }

    /**
     * @param  array<mixed>  $configuration
     */
    private function integer(
        array $configuration,
        string $setting,
        int $default,
        int $minimum,
        ?int $maximum = null,
    ): int {
        $value = $configuration[$setting] ?? $default;

        if (! is_int($value) || $value < $minimum || ($maximum !== null && $value > $maximum)) {
            $range = $maximum === null ? "at least $minimum" : "between $minimum and $maximum";

            throw RabbitMqConfigurationException::invalid(
                $this->connection,
                $setting,
                "must be an integer $range",
            );
        }

        return $value;
    }

    /**
     * @param  array<mixed>  $configuration
     */
    private function parseConsumerAcknowledgementTimeout(array $configuration): ?int
    {
        $seconds = $configuration['consumer_ack_timeout'] ?? null;

        if ($seconds === null || $seconds === '') {
            return null;
        }

        if (! is_int($seconds) || $seconds < 1 || $seconds > intdiv(self::MAX_AMQP_LONG, 1_000)) {
            throw RabbitMqConfigurationException::invalid(
                $this->connection,
                'consumer_ack_timeout',
                'must be a positive integer number of seconds that fits RabbitMQ\'s millisecond field',
            );
        }

        return $seconds * 1_000;
    }

    /**
     * @param  array<mixed>  $configuration
     */
    private function optionalReadableFile(
        string $setting,
        array $configuration,
        ?string $diagnosticSetting = null,
    ): ?string {
        $value = $configuration[$setting] ?? null;

        if ($value === null || $value === '') {
            return null;
        }

        if (! is_string($value) || ! is_file($value) || ! is_readable($value)) {
            throw RabbitMqConfigurationException::invalid(
                $this->connection,
                $diagnosticSetting ?? $setting,
                'must identify a readable CA bundle file',
            );
        }

        return $value;
    }
}
