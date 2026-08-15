<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\PubSub;

class ResourceName
{
    public static function topic(string $logicalTopic): string
    {
        return $logicalTopic;
    }

    public static function subscription(string $ownershipPrefix, string $logicalSubscription): string
    {
        return "$ownershipPrefix-$logicalSubscription";
    }

    public static function topicPath(string $projectId, string $logicalTopic): string
    {
        return "projects/$projectId/topics/".self::topic($logicalTopic);
    }

    public static function subscriptionId(string $resourceName): ?string
    {
        $prefix = '/subscriptions/';
        $position = strrpos($resourceName, $prefix);

        if ($position === false) {
            return null;
        }

        $subscription = substr($resourceName, $position + strlen($prefix));

        return $subscription !== '' && ! str_contains($subscription, '/')
            ? $subscription
            : null;
    }
}
