<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory;
use Spoolrail\Spoolrail\Exceptions\UnsupportedRabbitMqVersionException;

class RabbitMqConnectionFactory
{
    public function create(RabbitMqConnectionConfig $connection): AbstractConnection
    {
        $native = AMQPConnectionFactory::create($this->configuration($connection));

        $this->verifySupportedBroker($native);

        return $native;
    }

    /**
     * @internal
     */
    public function verifySupportedBroker(AbstractConnection $native): void
    {
        $properties = $native->getServerProperties();
        $versionProperty = $properties['version'] ?? null;
        $version = is_array($versionProperty) && isset($versionProperty[1]) && is_string($versionProperty[1])
            ? $versionProperty[1]
            : null;

        if (! is_string($version) || version_compare($version, RabbitMqVersion::MINIMUM, '<')) {
            $native->close();

            throw new UnsupportedRabbitMqVersionException(is_string($version) ? $version : 'unknown');
        }
    }

    /**
     * @internal
     */
    public function configuration(RabbitMqConnectionConfig $connection): AMQPConnectionConfig
    {
        $config = new AMQPConnectionConfig;
        $config->setHost($connection->host());
        $config->setPort($connection->port());
        $config->setUser($connection->username());
        $config->setPassword($connection->password());
        $config->setVhost($connection->virtualHost());

        $heartbeat = $connection->heartbeat();

        $config->setHeartbeat($heartbeat);
        $config->setKeepalive($heartbeat === 0);
        $config->setConnectionName("spoolrail:{$connection->connection}");

        $readWriteTimeout = max(3.0, $heartbeat * 2.0);

        $config->setReadTimeout($readWriteTimeout);
        $config->setWriteTimeout($readWriteTimeout);

        if ($connection->scheme() === 'amqps') {
            $config->setIsSecure(true);
            $config->setSslVerify(true);
            $config->setSslVerifyName(true);
            $config->setSslCaCert($connection->caFile());
        }

        return $config;
    }
}
