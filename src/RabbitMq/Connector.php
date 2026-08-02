<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Exception;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;

class Connector
{
    public function connect(ConnectionConfig $config): AbstractConnection
    {
        $hosts = $config->hosts();
        $lastHost = array_pop($hosts);

        foreach ($hosts as $host) {
            try {
                return $this->connectToHost($config, $host);
            } catch (Exception) {
                // Try the next configured host while establishing the connection.
            }
        }

        return $this->connectToHost($config, $lastHost);
    }

    /**
     * @internal
     */
    public function ensureVersionIsSupported(AbstractConnection $amqpConnection): void
    {
        $version = $this->serverVersion($amqpConnection);

        if (! is_string($version)) {
            $amqpConnection->close();

            throw RabbitMqTopologyException::unsupportedVersion('unknown');
        }

        if (version_compare($version, Version::MINIMUM, '<')) {
            $amqpConnection->close();

            throw RabbitMqTopologyException::unsupportedVersion($version);
        }
    }

    private function serverVersion(AbstractConnection $amqpConnection): ?string
    {
        $versionProperty = $amqpConnection->getServerProperties()['version'] ?? null;

        if (! is_array($versionProperty)) {
            return null;
        }

        $version = $versionProperty[1] ?? null;

        return is_string($version) ? $version : null;
    }

    /**
     * @internal
     */
    public function amqpConfiguration(ConnectionConfig $config, string $host): AMQPConnectionConfig
    {
        $amqpConfiguration = new AMQPConnectionConfig;
        $amqpConfiguration->setHost($host);
        $amqpConfiguration->setPort($config->port());
        $amqpConfiguration->setUser($config->username());
        $amqpConfiguration->setPassword($config->password());
        $amqpConfiguration->setVhost($config->virtualHost());
        $amqpConfiguration->setConnectionTimeout($config->connectionTimeout());

        $heartbeat = $config->heartbeat();

        $amqpConfiguration->setHeartbeat($heartbeat);
        $amqpConfiguration->setKeepalive($heartbeat === 0);
        $amqpConfiguration->setConnectionName("spoolrail:$config->connectionName");

        $readWriteTimeout = max(3.0, $heartbeat * 2.0);

        $amqpConfiguration->setReadTimeout($readWriteTimeout);
        $amqpConfiguration->setWriteTimeout($readWriteTimeout);

        if ($config->scheme() === 'amqps') {
            $amqpConfiguration->setIsSecure(true);
            $amqpConfiguration->setSslVerify(true);
            $amqpConfiguration->setSslVerifyName(true);
            $amqpConfiguration->setSslCaCert($config->caFile());
        }

        return $amqpConfiguration;
    }

    private function connectToHost(ConnectionConfig $config, string $host): AbstractConnection
    {
        $amqpConnection = AMQPConnectionFactory::create($this->amqpConfiguration($config, $host));

        $this->ensureVersionIsSupported($amqpConnection);

        return $amqpConnection;
    }
}
