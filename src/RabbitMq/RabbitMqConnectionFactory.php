<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Exception;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;

class RabbitMqConnectionFactory
{
    public function create(RabbitMqConnectionConfig $config): AbstractConnection
    {
        $hosts = $config->hosts();
        $lastHost = array_pop($hosts);

        foreach ($hosts as $host) {
            try {
                return $this->connect($config, $host);
            } catch (Exception) {
                // Try the next configured host while establishing the connection.
            }
        }

        return $this->connect($config, $lastHost);
    }

    /**
     * @internal
     */
    public function assertSupportedVersion(AbstractConnection $amqpConnection): void
    {
        $version = $this->serverVersion($amqpConnection);

        if (! is_string($version)) {
            $amqpConnection->close();

            throw RabbitMqTopologyException::unsupportedVersion('unknown');
        }

        if (version_compare($version, RabbitMqVersion::MINIMUM, '<')) {
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
    public function amqpConfig(RabbitMqConnectionConfig $config, string $host): AMQPConnectionConfig
    {
        $amqpConfig = new AMQPConnectionConfig;
        $amqpConfig->setHost($host);
        $amqpConfig->setPort($config->port());
        $amqpConfig->setUser($config->username());
        $amqpConfig->setPassword($config->password());
        $amqpConfig->setVhost($config->virtualHost());
        $amqpConfig->setConnectionTimeout($config->connectionTimeout());

        $heartbeat = $config->heartbeat();

        $amqpConfig->setHeartbeat($heartbeat);
        $amqpConfig->setKeepalive($heartbeat === 0);
        $amqpConfig->setConnectionName("spoolrail:$config->connectionName");

        $readWriteTimeout = max(3.0, $heartbeat * 2.0);

        $amqpConfig->setReadTimeout($readWriteTimeout);
        $amqpConfig->setWriteTimeout($readWriteTimeout);

        if ($config->scheme() === 'amqps') {
            $amqpConfig->setIsSecure(true);
            $amqpConfig->setSslVerify(true);
            $amqpConfig->setSslVerifyName(true);
            $amqpConfig->setSslCaCert($config->caFile());
        }

        return $amqpConfig;
    }

    private function connect(RabbitMqConnectionConfig $config, string $host): AbstractConnection
    {
        $amqpConnection = AMQPConnectionFactory::create($this->amqpConfig($config, $host));

        $this->assertSupportedVersion($amqpConnection);

        return $amqpConnection;
    }
}
