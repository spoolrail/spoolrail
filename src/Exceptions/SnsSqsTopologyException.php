<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use Aws\Exception\AwsException;
use RuntimeException;
use Throwable;

class SnsSqsTopologyException extends RuntimeException implements SpoolrailException
{
    public static function operationFailed(string $operation, Throwable $previous): self
    {
        return new self("AWS topology operation failed while $operation.", previous: $previous);
    }

    public static function topicMissing(string $topic): self
    {
        return new self("AWS SNS topic [$topic] does not exist.");
    }

    public static function topicHasSubscriptions(string $topic): self
    {
        return new self("AWS SNS topic [$topic] cannot be deleted while it has subscriptions.");
    }

    public static function incompatibleTopic(string $topic, string $reason): self
    {
        return new self("AWS SNS topic [$topic] is incompatible with Spoolrail: $reason.");
    }

    public static function incompatibleQueue(string $queue, string $reason): self
    {
        return new self("AWS SQS queue [$queue] is incompatible with Spoolrail: $reason.");
    }

    public static function conflictingQueueType(string $expected, string $existing): self
    {
        return new self(
            "AWS SQS queue [$existing] represents this subscription with a different queue type from expected queue [$expected]. Changing [fifo] selects replacement topology; use a replacement connection and subscription name, or fully drain and remove the old queue before synchronizing.",
        );
    }

    public static function invalidQueuePolicy(string $queue): self
    {
        return new self("AWS SQS queue [$queue] has a policy Spoolrail cannot read safely.");
    }

    public static function conflictingQueuePolicy(string $queue): self
    {
        return new self("AWS SQS queue [$queue] has a conflicting [SpoolrailSnsPublish] policy statement.");
    }

    public static function missingQueueRoute(string $queue): self
    {
        return new self("AWS SQS queue [$queue] has no Spoolrail SNS source in its policy, so its subscription cannot be removed safely.");
    }

    public static function incompatibleSubscription(string $queue): self
    {
        return new self("The AWS SNS subscription for SQS queue [$queue] must enable raw message delivery.");
    }

    public static function creationAccountMismatch(string $expected, string $actual): self
    {
        return new self(
            "AWS topology creation requires credentials for resource-owning account [$expected], but the current identity belongs to account [$actual]. Assume a role in the owning account before synchronizing.",
        );
    }

    public static function unexpectedCreatedResource(string $expected, string $actual): self
    {
        return new self("AWS created resource [$actual] while Spoolrail expected [$expected].");
    }

    public function shouldRetry(): bool
    {
        $failure = $this->getPrevious();

        if (! $failure instanceof AwsException) {
            return false;
        }

        $status = $failure->getStatusCode();

        if (in_array($status, [null, 408, 429], true) || $status >= 500) {
            return true;
        }

        $errorCode = (string) $failure->getAwsErrorCode();

        return stripos($errorCode, 'throttl') !== false
            || in_array($errorCode, ['RequestLimitExceeded', 'TooManyRequestsException'], true);
    }
}
