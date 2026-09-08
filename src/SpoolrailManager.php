<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail;

use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use Aws\Sts\StsClient;
use Closure;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\V1\Client\SubscriberClient;
use GuzzleHttp\Handler\CurlMultiHandler;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Container\BindingResolutionException;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Http\Client\Factory as HttpFactory;
use LogicException;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Drivers\ArrayDriver;
use Spoolrail\Spoolrail\Drivers\PubSubDriver;
use Spoolrail\Spoolrail\Drivers\RabbitMqDriver;
use Spoolrail\Spoolrail\Drivers\SnsSqsDriver;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig as PubSubConnectionConfig;
use Spoolrail\Spoolrail\PubSub\Topology as PubSubTopology;
use Spoolrail\Spoolrail\RabbitMq\ConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\Connector;
use Spoolrail\Spoolrail\RabbitMq\ManagementClient;
use Spoolrail\Spoolrail\RabbitMq\Topology;
use Spoolrail\Spoolrail\SnsSqs\ConnectionConfig as SnsSqsConnectionConfig;
use Spoolrail\Spoolrail\SnsSqs\QueuePolicy as SnsSqsQueuePolicy;
use Spoolrail\Spoolrail\SnsSqs\Topology as SnsSqsTopology;
use Spoolrail\Spoolrail\Subscriptions\ConsumerConfig;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Topology\OwnershipPrefix;

class SpoolrailManager
{
    /**
     * @var array<string, Connection>
     */
    private array $connections = [];

    /**
     * @var array<string, Closure(Application, array<array-key, mixed>, string): Driver<covariant mixed>>
     */
    private array $customCreators = [];

    /**
     * @var (Closure(array<string, string>): array<string, string>)|null
     */
    private ?Closure $transformHeadersCallback = null;

    /** @var (Closure(array<array-key, mixed>, TransportContext): bool)|null */
    private ?Closure $discardInvalidMessagesCallback = null;

    public function __construct(
        private Application $app,
        private Repository $config,
        private MessageEnvelope $envelope,
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

    /**
     * @param  Closure(array<string, string>): array<string, string>  $callback
     */
    public function transformHeadersUsing(Closure $callback): void
    {
        $this->transformHeadersCallback = $callback;
    }

    /** @param Closure(array<array-key, mixed>, TransportContext): bool $callback */
    public function discardInvalidMessagesWhen(Closure $callback): void
    {
        $this->discardInvalidMessagesCallback = $callback;
    }

    /**
     * @internal
     *
     * @param  array<array-key, mixed>  $envelope
     */
    public function shouldDiscardInvalidMessage(array $envelope, TransportContext $transport): bool
    {
        return $this->discardInvalidMessagesCallback instanceof Closure
            && ($this->discardInvalidMessagesCallback)($envelope, $transport) === true;
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

    public function driverName(?string $connectionName = null): string
    {
        $connectionName ??= $this->defaultConnectionName();

        return $this->driverNameFrom(
            $connectionName,
            $this->connectionConfig($connectionName),
        );
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
        $driverName = $this->driverNameFrom($connectionName, $connectionConfig);
        $creator = $this->customCreators[$driverName] ?? null;

        if (! $creator instanceof Closure && ! in_array($driverName, ['array', 'rabbitmq', 'snssqs', 'pubsub'], true)) {
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
            transformHeadersCallback: fn (array $headers): mixed => $this->transformHeaders($headers),
        );
    }

    /**
     * @param  array<string, string>  $headers
     */
    private function transformHeaders(array $headers): mixed
    {
        return $this->transformHeadersCallback instanceof Closure
            ? ($this->transformHeadersCallback)($headers)
            : $headers;
    }

    private function outboxEnabled(): bool
    {
        return $this->config->get('spoolrail.outbox.enabled', false) === true;
    }

    /**
     * @param  array<array-key, mixed>  $connectionConfig
     * @param  (Closure(Application, array<array-key, mixed>, string): Driver<covariant mixed>)|null  $creator
     * @return Driver<covariant mixed>
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
     * @return Driver<covariant mixed>
     */
    private function createBuiltInDriver(
        string $connectionName,
        array $connectionConfig,
        string $driverName,
    ): Driver {
        return match ($driverName) {
            'array' => $this->createArrayDriver($connectionName),
            'rabbitmq' => $this->createRabbitMqDriver($connectionName, $connectionConfig),
            'snssqs' => $this->createSnsSqsDriver($connectionName, $connectionConfig),
            'pubsub' => $this->createPubSubDriver($connectionName, $connectionConfig),
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
    private function driverNameFrom(string $connectionName, array $connectionConfig): string
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
            $this->app->make(ConsumerConfig::class)->idleWaitMilliseconds(),
        );
    }

    /**
     * @param  array<array-key, mixed>  $config
     */
    private function createSnsSqsDriver(string $connectionName, array $config): SnsSqsDriver
    {
        if (! class_exists(SnsClient::class)) {
            throw new LogicException(
                'The AWS SNS/SQS driver requires aws/aws-sdk-php:^3.392.0. Install it in the application before selecting this driver.',
            );
        }

        $connectionConfig = new SnsSqsConnectionConfig($connectionName, $config);
        $singleAttemptClientOptions = $connectionConfig->singleAttemptClientOptions();
        $singleAttemptSns = new SnsClient($singleAttemptClientOptions);
        $idleWaitMilliseconds = $this->app->make(ConsumerConfig::class)
            ->idleWaitMilliseconds();
        $httpHandler = $this->newConsumerHttpHandler($idleWaitMilliseconds);
        $consumerOptions = $connectionConfig->clientOptions();
        $consumerOptions['http_handler'] = $httpHandler;

        $consumerSqs = new SqsClient($consumerOptions);
        $topology = new SnsSqsTopology(
            $connectionConfig,
            $singleAttemptSns,
            new SqsClient($singleAttemptClientOptions),
            new StsClient($singleAttemptClientOptions),
            new SnsSqsQueuePolicy,
        );

        return new SnsSqsDriver(
            $connectionConfig,
            $singleAttemptSns,
            $consumerSqs,
            $topology,
            $this->app->make(OwnershipPrefix::class),
            $httpHandler,
            $idleWaitMilliseconds,
        );
    }

    /**
     * @param  array<array-key, mixed>  $config
     */
    private function createPubSubDriver(string $connectionName, array $config): PubSubDriver
    {
        if (! class_exists(PubSubClient::class)) {
            throw new LogicException(
                'The Google Pub/Sub driver requires google/cloud-pubsub:^2.20.0. Install it in the application before selecting this driver.',
            );
        }

        $connectionConfig = new PubSubConnectionConfig($connectionName, $config);
        $singleAttemptClient = new PubSubClient($connectionConfig->singleAttemptClientOptions());
        $idleWaitMilliseconds = $this->app->make(ConsumerConfig::class)
            ->idleWaitMilliseconds();
        $httpHandler = $this->newConsumerHttpHandler($idleWaitMilliseconds);

        return new PubSubDriver(
            $connectionConfig,
            $singleAttemptClient,
            new SubscriberClient($connectionConfig->subscriberClientOptions($httpHandler)),
            new PubSubTopology($connectionConfig, $singleAttemptClient),
            $this->app->make(OwnershipPrefix::class),
            $httpHandler,
            $idleWaitMilliseconds,
        );
    }

    private function newConsumerHttpHandler(int $idleWaitMilliseconds): CurlMultiHandler
    {
        return new CurlMultiHandler([
            'select_timeout' => $idleWaitMilliseconds / 1_000,
        ]);
    }
}
