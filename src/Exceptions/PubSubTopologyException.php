<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use Google\Cloud\Core\Exception\ServiceException;
use Google\Rpc\Code;
use RuntimeException;
use Throwable;

class PubSubTopologyException extends RuntimeException implements SpoolrailException
{
    public static function operationFailed(string $operation, Throwable $previous): self
    {
        return new self("Google Pub/Sub topology operation failed while $operation.", previous: $previous);
    }

    public static function topicMissing(string $topic): self
    {
        return new self("Google Pub/Sub topic [$topic] does not exist.");
    }

    public static function topicHasSubscriptions(string $topic): self
    {
        return new self("Google Pub/Sub topic [$topic] cannot be deleted while it has subscriptions.");
    }

    public static function incompatibleSubscription(string $subscription, string $reason): self
    {
        return new self("Google Pub/Sub subscription [$subscription] is incompatible with Spoolrail: $reason.");
    }

    public static function immutableOrdering(
        string $subscription,
        bool $actual,
        bool $expected,
    ): self {
        $actualState = $actual ? 'enabled' : 'disabled';
        $expectedState = $expected ? 'enabled' : 'disabled';

        return self::incompatibleSubscription(
            $subscription,
            "message ordering is [$actualState] while this connection requires [$expectedState]; message ordering cannot be changed after creation; use a replacement subscription name, synchronize it, and drain the existing subscription before deleting it",
        );
    }

    public function shouldRetry(): bool
    {
        $failure = $this->getPrevious();

        return $failure instanceof ServiceException
            && in_array($failure->getCode(), [
                Code::CANCELLED,
                Code::UNKNOWN,
                Code::DEADLINE_EXCEEDED,
                Code::RESOURCE_EXHAUSTED,
                Code::ABORTED,
                Code::INTERNAL,
                Code::UNAVAILABLE,
            ], true);
    }
}
