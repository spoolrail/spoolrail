<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqVersion;

class RabbitMqTopologyException extends RuntimeException implements SpoolrailException
{
    public static function unsupportedVersion(string $version): self
    {
        return new self("RabbitMQ [$version] is not supported; Spoolrail requires RabbitMQ ".RabbitMqVersion::MINIMUM.' or later.');
    }

    public static function incompatibleExchange(string $exchange, string $reason): self
    {
        return new self("RabbitMQ exchange [$exchange] is incompatible: $reason.");
    }

    public static function incompatibleQueue(string $queue, string $reason): self
    {
        return new self("RabbitMQ queue [$queue] is incompatible: $reason.");
    }

    public static function incompatibleBindings(string $queue, string $topic): self
    {
        return new self(
            "RabbitMQ queue [$queue] must have only one non-default exchange binding, from topic [$topic].",
        );
    }

    public static function unsupportedDefaultQueueType(string $type): self
    {
        return new self(
            "RabbitMQ virtual host default queue type [$type] is incompatible; Spoolrail supports classic and quorum queues.",
        );
    }

    public static function finiteDeliveryLimit(string $queue, int $limit): self
    {
        return new self(
            "RabbitMQ quorum queue [$queue] has an effective finite delivery limit of $limit; Spoolrail requires unlimited delivery attempts.",
        );
    }

    public static function indeterminateDeliveryLimit(string $queue): self
    {
        return new self(
            "RabbitMQ quorum queue [$queue] does not have a verifiable unlimited delivery limit.",
        );
    }

    public static function invalidPolicy(string $policy): self
    {
        return new self("RabbitMQ policy [$policy] cannot be evaluated safely.");
    }

    public static function topicMissing(string $topic): self
    {
        return new self("RabbitMQ topic [$topic] does not exist.");
    }

    public static function topicHasBindings(string $topic): self
    {
        return new self("RabbitMQ topic [$topic] cannot be deleted while it has bindings.");
    }
}
