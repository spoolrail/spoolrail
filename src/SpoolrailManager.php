<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Closure;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Http\Client\Factory as HttpFactory;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Drivers\RabbitMqDriver;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\MissingRabbitMqDependencyException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqConfigException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionFactory;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqManagementClient;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqTopology;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

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
        private readonly MessageSerializer $serializer,
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
    public function potentiallyManagedConnectionNames(): array
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

        if (isset($this->customCreators[$driverName])) {
            $driver = $this->customCreators[$driverName]($this->app, $connectionConfig, $connectionName);
        } else {
            $driver = match ($driverName) {
                'array' => $this->createArrayDriver($connectionName),
                'rabbitmq' => $this->createRabbitMqDriver($connectionName, $connectionConfig),
                default => throw InvalidConfigException::unsupportedDriver($driverName),
            };
        }

        return new Connection($driver, $this->serializer);
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
     * @throws MissingRabbitMqDependencyException
     * @throws RabbitMqConfigException
     */
    private function createRabbitMqDriver(string $connectionName, array $config): RabbitMqDriver
    {
        if (! class_exists(AMQPConnectionConfig::class)) {
            throw new MissingRabbitMqDependencyException;
        }

        $connectionConfig = new RabbitMqConnectionConfig($connectionName, $config);
        $managementClient = new RabbitMqManagementClient(
            $connectionConfig,
            $this->app->make(HttpFactory::class),
        );

        return new RabbitMqDriver(
            $connectionConfig,
            new RabbitMqConnectionFactory,
            new RabbitMqTopology($connectionConfig, $managementClient),
            $this->app->make(OwnershipPrefix::class),
        );
    }
}
