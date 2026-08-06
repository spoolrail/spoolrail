<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Closure;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Http\Client\Factory as HttpFactory;
use LogicException;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Drivers\RabbitMqDriver;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\RabbitMq\ConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\Connector;
use Spoolrail\Spoolrail\RabbitMq\ManagementClient;
use Spoolrail\Spoolrail\RabbitMq\Topology;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

class SpoolrailManager
{
    /**
     * @var array<string, Connection>
     */
    private array $connections = [];

    /**
     * @var array<string, Closure(Application, array<array-key, mixed>, string): Driver>
     */
    private array $customCreators = [];

    public function __construct(
        private readonly Application $app,
        private readonly Repository $config,
        private readonly MessageEnvelope $envelope,
    ) {}

    public function connection(?string $name = null): Connection
    {
        $name ??= $this->defaultConnectionName();

        return $this->connections[$name] ??= $this->resolve($name);
    }

    public function extend(string $driver, Closure $creator): static
    {
        $this->customCreators[$driver] = $creator;

        return $this;
    }

    public function forgetConnection(?string $name = null): void
    {
        $name ??= $this->defaultConnectionName();

        if (isset($this->connections[$name])) {
            $this->connections[$name]->close();
        }

        unset($this->connections[$name]);
    }

    /**
     * @param  array<int, mixed>  $parameters
     */
    public function __call(string $method, array $parameters): mixed
    {
        return $this->connection()->$method(...$parameters);
    }

    public function defaultConnectionName(): string
    {
        $connectionName = $this->config->get('spoolrail.default');

        if (! is_string($connectionName) || trim($connectionName) === '') {
            throw InvalidConfigException::invalidDefaultConnection();
        }

        return $connectionName;
    }

    /**
     * @return list<string>
     */
    public function configuredConnectionNames(): array
    {
        $configuredConnections = $this->config->get('spoolrail.connections');

        if (! is_array($configuredConnections)) {
            return [];
        }

        return array_values(array_filter(array_keys($configuredConnections), is_string(...)));
    }

    /**
     * @return list<string>
     */
    public function connectionNamesThatMayManageTopology(): array
    {
        return array_values(array_filter(
            $this->configuredConnectionNames(),
            function (string $connectionName): bool {
                $connectionConfig = $this->config->get("spoolrail.connections.$connectionName");

                return is_array($connectionConfig) && ($connectionConfig['driver'] ?? null) !== 'array';
            },
        ));
    }

    private function resolve(string $connectionName): Connection
    {
        $connectionConfig = $this->connectionConfig($connectionName);
        $driverName = $this->driverName($connectionName, $connectionConfig);
        $creator = $this->customCreators[$driverName] ?? null;

        if (! $creator instanceof Closure && ! in_array($driverName, ['array', 'rabbitmq'], true)) {
            throw InvalidConfigException::unsupportedDriver($driverName);
        }

        $resolveDriver = fn (): Driver => $this->createDriver(
            $connectionName,
            $connectionConfig,
            $driverName,
            $creator,
        );

        return new Connection(
            driver: $this->outboxEnabled() ? $resolveDriver : $resolveDriver(),
            envelope: $this->envelope,
            connectionName: $connectionName,
        );
    }

    private function outboxEnabled(): bool
    {
        return $this->config->get('spoolrail.outbox.enabled', false) === true;
    }

    /**
     * @param  array<array-key, mixed>  $connectionConfig
     * @param  (Closure(Application, array<array-key, mixed>, string): Driver)|null  $creator
     */
    private function createDriver(
        string $connectionName,
        array $connectionConfig,
        string $driverName,
        ?Closure $creator,
    ): Driver {
        if ($creator instanceof Closure) {
            return $creator($this->app, $connectionConfig, $connectionName);
        }

        return $this->createBuiltInDriver($connectionName, $connectionConfig, $driverName);
    }

    /**
     * @param  array<array-key, mixed>  $connectionConfig
     */
    private function createBuiltInDriver(
        string $connectionName,
        array $connectionConfig,
        string $driverName,
    ): Driver {
        return match ($driverName) {
            'array' => $this->createArrayDriver($connectionName),
            'rabbitmq' => $this->createRabbitMqDriver($connectionName, $connectionConfig),
            default => throw InvalidConfigException::unsupportedDriver($driverName),
        };
    }

    /**
     * @return array<array-key, mixed>
     */
    private function connectionConfig(string $connectionName): array
    {
        $connectionConfig = $this->config->get("spoolrail.connections.$connectionName");

        if ($connectionConfig === null) {
            throw InvalidConfigException::undefinedConnection($connectionName);
        }

        if (! is_array($connectionConfig)) {
            throw InvalidConfigException::connectionMustBeArray($connectionName);
        }

        return $connectionConfig;
    }

    /**
     * @param  array<array-key, mixed>  $connectionConfig
     */
    private function driverName(string $connectionName, array $connectionConfig): string
    {
        $driver = $connectionConfig['driver'] ?? null;

        if (! is_string($driver) || trim($driver) === '') {
            throw InvalidConfigException::missingDriver($connectionName);
        }

        return $driver;
    }

    private function createArrayDriver(string $connectionName): ArrayDriver
    {
        return new ArrayDriver(
            $connectionName,
            $this->defaultConnectionName(),
            $this->app->make(SubscriptionRegistry::class),
        );
    }

    /**
     * @param  array<array-key, mixed>  $config
     *
     * @throws BindingResolutionException
     * @throws InvalidConfigException
     * @throws LogicException
     */
    private function createRabbitMqDriver(string $connectionName, array $config): RabbitMqDriver
    {
        if (! class_exists(AMQPConnectionConfig::class)) {
            throw new LogicException(
                'The RabbitMQ driver requires php-amqplib/php-amqplib:^3.7.4. Install it in the application before selecting this driver.',
            );
        }

        $connectionConfig = new ConnectionConfig($connectionName, $config);
        $managementClient = new ManagementClient(
            $connectionConfig,
            $this->app->make(HttpFactory::class),
        );

        return new RabbitMqDriver(
            $connectionConfig,
            new Connector,
            new Topology($connectionConfig, $managementClient),
            $this->app->make(OwnershipPrefix::class),
        );
    }
}
