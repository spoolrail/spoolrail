<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Closure;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Foundation\Application;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigurationException;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

class SpoolrailManager
{
    /**
     * @var array<string, Connection>
     */
    private array $connections = [];

    /**
     * @var array<string, Closure(Application, array<mixed>, string): Driver>
     */
    private array $customCreators = [];

    public function __construct(
        private readonly Application $app,
        private readonly Repository $config,
        private readonly MessageSerializer $serializer,
    ) {}

    public function connection(?string $name = null): Connection
    {
        $name ??= $this->getDefaultConnection();

        return $this->connections[$name] ??= $this->resolve($name);
    }

    public function extend(string $driver, Closure $creator): static
    {
        $this->customCreators[$driver] = $creator;

        return $this;
    }

    public function forgetConnection(?string $name = null): void
    {
        $name ??= $this->getDefaultConnection();

        unset($this->connections[$name]);
    }

    /**
     * @param  array<int, mixed>  $parameters
     */
    public function __call(string $method, array $parameters): mixed
    {
        return $this->connection()->$method(...$parameters);
    }

    public function getDefaultConnection(): string
    {
        $name = $this->config->get('spoolrail.default');

        if (! is_string($name) || trim($name) === '') {
            throw InvalidConfigurationException::invalidDefaultConnection();
        }

        return $name;
    }

    private function resolve(string $name): Connection
    {
        $config = $this->connectionConfig($name);
        $driver = $this->driverName($name, $config);

        if (isset($this->customCreators[$driver])) {
            $instance = $this->customCreators[$driver]($this->app, $config, $name);
        } else {
            $instance = match ($driver) {
                'array' => $this->createArrayDriver($name),
                default => throw InvalidConfigurationException::unsupportedDriver($driver),
            };
        }

        return new Connection($instance, $this->serializer);
    }

    /**
     * @return array<mixed>
     */
    private function connectionConfig(string $name): array
    {
        $config = $this->config->get("spoolrail.connections.$name");

        if ($config === null) {
            throw InvalidConfigurationException::undefinedConnection($name);
        }

        if (! is_array($config)) {
            throw InvalidConfigurationException::connectionMustBeArray($name);
        }

        return $config;
    }

    /**
     * @param  array<mixed>  $config
     */
    private function driverName(string $connection, array $config): string
    {
        $driver = $config['driver'] ?? null;

        if (! is_string($driver) || trim($driver) === '') {
            throw InvalidConfigurationException::missingDriver($connection);
        }

        return $driver;
    }

    private function createArrayDriver(string $name): ArrayDriver
    {
        return new ArrayDriver(
            $name,
            $this->getDefaultConnection(),
            $this->app->make(SubscriptionRegistry::class),
        );
    }
}
