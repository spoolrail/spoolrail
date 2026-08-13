<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\SnsSqs;

use Aws\Endpoint\PartitionEndpointProvider;
use LengthException;

class ResourceName
{
    public const int MAX_TOPIC_BYTES = 256;

    public const int MAX_QUEUE_BYTES = 80;

    public static function topic(string $logicalTopic, bool $fifo): string
    {
        $topic = $fifo ? "$logicalTopic.fifo" : $logicalTopic;

        if (strlen($topic) > self::MAX_TOPIC_BYTES) {
            throw new LengthException(
                "Logical topic [$logicalTopic] derives AWS SNS topic [$topic], which exceeds the ".self::MAX_TOPIC_BYTES.'-byte transport limit.',
            );
        }

        return $topic;
    }

    public static function queue(
        string $ownershipPrefix,
        string $subscription,
        bool $fifo,
    ): string {
        $queue = "$ownershipPrefix-$subscription".($fifo ? '.fifo' : '');

        if (strlen($queue) > self::MAX_QUEUE_BYTES) {
            throw new LengthException(
                "Logical subscription [$subscription] with ownership prefix [$ownershipPrefix] derives AWS SQS queue [$queue], which exceeds the ".self::MAX_QUEUE_BYTES.'-byte transport limit.',
            );
        }

        return $queue;
    }

    public static function topicArn(
        ConnectionConfig $config,
        string $logicalTopic,
    ): string {
        return self::arn(
            $config,
            'sns',
            self::topic($logicalTopic, $config->fifo()),
        );
    }

    public static function queueArn(
        ConnectionConfig $config,
        string $physicalQueue,
    ): string {
        return self::arn($config, 'sqs', $physicalQueue);
    }

    private static function arn(
        ConnectionConfig $config,
        string $service,
        string $resource,
    ): string {
        $partition = PartitionEndpointProvider::defaultProvider()
            ->getPartition($config->region(), $service)
            ->getName();

        return "arn:$partition:$service:{$config->region()}:{$config->accountId()}:$resource";
    }
}
