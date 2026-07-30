<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Spoolrail\Spoolrail\Exceptions\RabbitMqQueueNameTooLongException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopicNameTooLongException;

class RabbitMqName
{
    public const int MAX_BYTES = 255;

    public static function queue(string $ownershipPrefix, string $subscription): string
    {
        $queueName = "$ownershipPrefix-$subscription";

        if (strlen($queueName) > self::MAX_BYTES) {
            throw new RabbitMqQueueNameTooLongException(
                $subscription,
                $queueName,
                $ownershipPrefix,
                self::MAX_BYTES,
            );
        }

        return $queueName;
    }

    public static function topic(string $topic): string
    {
        if (strlen($topic) > self::MAX_BYTES) {
            throw new RabbitMqTopicNameTooLongException($topic, self::MAX_BYTES);
        }

        return $topic;
    }
}
