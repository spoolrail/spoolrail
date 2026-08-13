<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Concerns;

use Aws\Exception\AwsException;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use Spoolrail\Spoolrail\SpoolrailManager;

trait InteractsWithSnsSqs
{
    private string $awsAccountId;

    private string $awsRegion;

    private SnsClient $sns;

    private SqsClient $sqs;

    protected function setUpInteractsWithSnsSqs(): void
    {
        $this->awsAccountId = (string) random_int(100_000_000_000, 999_999_999_999);
        $this->awsRegion = config('spoolrail.connections.snssqs.region');

        config()->set('spoolrail.connections.snssqs', [
            'driver' => 'snssqs',
            'key' => $this->awsAccountId,
            'secret' => 'test',
            'token' => null,
            'region' => $this->awsRegion,
            'account_id' => $this->awsAccountId,
            'endpoint' => 'http://127.0.0.1:4566',
            'fifo' => true,
            'connection_timeout' => 3,
            'request_timeout' => 60,
        ]);

        $options = [
            'version' => 'latest',
            'region' => $this->awsRegion,
            'endpoint' => 'http://127.0.0.1:4566',
            'credentials' => [
                'key' => $this->awsAccountId,
                'secret' => 'test',
            ],
        ];

        $this->sns = new SnsClient($options);
        $this->sqs = new SqsClient($options);
    }

    protected function tearDownInteractsWithSnsSqs(): void
    {
        app(SpoolrailManager::class)->forgetConnection('snssqs');

        foreach ($this->sns->listSubscriptions()->get('Subscriptions') ?? [] as $subscription) {
            $arn = is_array($subscription) ? ($subscription['SubscriptionArn'] ?? null) : null;

            if (is_string($arn) && str_starts_with($arn, 'arn:')) {
                $this->sns->unsubscribe(['SubscriptionArn' => $arn]);
            }
        }

        foreach ($this->sqs->listQueues()->get('QueueUrls') ?? [] as $queueUrl) {
            if (is_string($queueUrl)) {
                $this->sqs->deleteQueue(['QueueUrl' => $queueUrl]);
            }
        }

        foreach ($this->sns->listTopics()->get('Topics') ?? [] as $topic) {
            $arn = is_array($topic) ? ($topic['TopicArn'] ?? null) : null;

            if (is_string($arn)) {
                $this->sns->deleteTopic(['TopicArn' => $arn]);
            }
        }
    }

    protected function snsTopicArn(string $topic, bool $fifo = true): string
    {
        $name = $topic.($fifo ? '.fifo' : '');

        return "arn:aws:sns:$this->awsRegion:$this->awsAccountId:$name";
    }

    protected function sqsQueueName(
        string $subscription,
        bool $fifo = true,
        ?string $ownershipPrefix = null,
    ): string {
        return ($ownershipPrefix ?? config('spoolrail.prefix'))."-$subscription".($fifo ? '.fifo' : '');
    }

    protected function sqsQueueUrl(
        string $subscription,
        bool $fifo = true,
        ?string $ownershipPrefix = null,
    ): string {
        return (string) $this->sqs->getQueueUrl([
            'QueueName' => $this->sqsQueueName($subscription, $fifo, $ownershipPrefix),
            'QueueOwnerAWSAccountId' => $this->awsAccountId,
        ])->get('QueueUrl');
    }

    protected function sqsQueueExists(
        string $subscription,
        bool $fifo = true,
        ?string $ownershipPrefix = null,
    ): bool {
        try {
            $this->sqsQueueUrl($subscription, $fifo, $ownershipPrefix);

            return true;
        } catch (AwsException $exception) {
            if ($exception->getStatusCode() === 404 || str_contains((string) $exception->getAwsErrorCode(), 'NonExistent')) {
                return false;
            }

            throw $exception;
        }
    }

    protected function snsTopicExists(string $topic, bool $fifo = true): bool
    {
        try {
            $this->snsTopicAttributes($topic, $fifo);

            return true;
        } catch (AwsException $exception) {
            if ($exception->getStatusCode() === 404 || str_contains((string) $exception->getAwsErrorCode(), 'NotFound')) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * @return array<string, string>
     */
    protected function snsTopicAttributes(string $topic, bool $fifo = true): array
    {
        return $this->sns->getTopicAttributes([
            'TopicArn' => $this->snsTopicArn($topic, $fifo),
        ])->get('Attributes') ?? [];
    }

    /**
     * @return array<string, string>
     */
    protected function sqsQueueAttributes(string $subscription, bool $fifo = true): array
    {
        return $this->sqs->getQueueAttributes([
            'QueueUrl' => $this->sqsQueueUrl($subscription, $fifo),
            'AttributeNames' => ['All'],
        ])->get('Attributes') ?? [];
    }
}
