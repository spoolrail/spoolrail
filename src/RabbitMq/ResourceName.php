<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use LengthException;

class ResourceName
{
    public const int MAX_BYTES = 255;

    public static function queue(string $ownershipPrefix, string $subscription): string
    {
        $queueName = "$ownershipPrefix-$subscription";

        if (strlen($queueName) > self::MAX_BYTES) {
            throw new LengthException(
                "Logical subscription [$subscription] with ownership prefix [$ownershipPrefix] derives RabbitMQ queue [$queueName], which exceeds the ".self::MAX_BYTES.'-byte transport limit.',
            );
        }

        return $queueName;
    }

    public static function topic(string $topic): string
    {
        if (strlen($topic) > self::MAX_BYTES) {
            throw new LengthException(
                "Logical topic [$topic] is also its RabbitMQ exchange name and exceeds the ".self::MAX_BYTES.'-byte transport limit.',
            );
        }

        return $topic;
    }
}
