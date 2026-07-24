<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Spoolrail\Spoolrail\Exceptions\InvalidPhysicalNameException;
use Spoolrail\Spoolrail\Exceptions\InvalidRabbitMqTopicNameException;

class RabbitMqName
{
    public const MAX_BYTES = 255;

    public static function queue(string $ownershipPrefix, string $subscription): string
    {
        $physicalName = "$ownershipPrefix-$subscription";

        if (strlen($physicalName) > self::MAX_BYTES) {
            throw new InvalidPhysicalNameException(
                $subscription,
                $physicalName,
                $ownershipPrefix,
                self::MAX_BYTES,
            );
        }

        return $physicalName;
    }

    public static function topic(string $topic): string
    {
        if (strlen($topic) > self::MAX_BYTES) {
            throw new InvalidRabbitMqTopicNameException($topic, self::MAX_BYTES);
        }

        return $topic;
    }
}
