<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Concerns;

use Aws\Exception\AwsException;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use RuntimeException;
use Spoolrail\Spoolrail\SnsSqs\ConnectionConfig;
use Spoolrail\Spoolrail\SnsSqs\ResourceName;
use Spoolrail\Spoolrail\SpoolrailManager;
use Throwable;

trait InteractsWithExternalSnsSqs
{
    use InteractsWithExternalServices;

    private ConnectionConfig $externalSnsSqsConfig;

    private SnsClient $externalSns;

    private SqsClient $externalSqs;

    protected function setUpInteractsWithExternalSnsSqs(): void
    {
        $this->setUpExternalTestEnvironment();

        config()->set('spoolrail.connections.snssqs', $this->externalSnsSqsConnection());

        $this->externalSnsSqsConfig = new ConnectionConfig(
            'snssqs',
            config('spoolrail.connections.snssqs'),
        );
        $options = $this->externalSnsSqsConfig->singleAttemptClientOptions();
        $this->externalSns = new SnsClient($options);
        $this->externalSqs = new SqsClient($options);

        $this->deleteExternalSnsSqsResources();
    }

    protected function tearDownInteractsWithExternalSnsSqs(): void
    {
        app(SpoolrailManager::class)->forgetConnection('snssqs');

        $this->deleteExternalSnsSqsResources();
    }

    /**
     * @return array<string, string>
     */
    protected function externalSnsTopicAttributes(): array
    {
        return $this->externalSns->getTopicAttributes([
            'TopicArn' => ResourceName::topicArn(
                $this->externalSnsSqsConfig,
                $this->externalTopic,
            ),
        ])->get('Attributes') ?? [];
    }

    /**
     * @return array<string, string>
     */
    protected function externalSqsQueueAttributes(): array
    {
        return $this->externalSqs->getQueueAttributes([
            'QueueUrl' => $this->externalSqsQueueUrl(),
            'AttributeNames' => ['All'],
        ])->get('Attributes') ?? [];
    }

    private function externalSqsQueueUrl(): string
    {
        return (string) $this->externalSqs->getQueueUrl([
            'QueueName' => ResourceName::queue(
                $this->externalPrefix,
                $this->externalSubscription,
                true,
            ),
            'QueueOwnerAWSAccountId' => $this->externalSnsSqsConfig->accountId(),
        ])->get('QueueUrl');
    }

    /**
     * @return array<string, mixed>
     */
    private function externalSnsSqsConnection(): array
    {
        return [
            'driver' => 'snssqs',
            'key' => $this->requiredExternalEnvironment('AWS_ACCESS_KEY_ID'),
            'secret' => $this->requiredExternalEnvironment('AWS_SECRET_ACCESS_KEY'),
            'region' => $this->requiredExternalEnvironment('AWS_DEFAULT_REGION'),
            'account_id' => $this->requiredExternalEnvironment('AWS_ACCOUNT_ID'),
            'endpoint' => null,
            'fifo' => true,
            'connection_timeout' => 3,
            'request_timeout' => 60,
        ];
    }

    private function deleteExternalSnsSqsResources(): void
    {
        $failures = [];

        try {
            $topicArns = $this->externalSnsTopicArns();
        } catch (Throwable $exception) {
            $failures[] = $exception;
            $topicArns = [];
        }

        foreach ($topicArns as $topicArn) {
            try {
                $this->externalSns->deleteTopic(['TopicArn' => $topicArn]);
            } catch (Throwable $exception) {
                $failures[] = $exception;
            }
        }

        try {
            $queueUrls = $this->externalSqsQueueUrls();
        } catch (Throwable $exception) {
            $failures[] = $exception;
            $queueUrls = [];
        }

        foreach ($queueUrls as $queueUrl) {
            try {
                $this->externalSqs->deleteQueue(['QueueUrl' => $queueUrl]);
            } catch (Throwable $exception) {
                if (
                    $exception instanceof AwsException
                    && $exception->getAwsErrorCode() === 'AWS.SimpleQueueService.NonExistentQueue'
                ) {
                    continue;
                }

                $failures[] = $exception;
            }
        }

        if ($failures !== []) {
            throw new RuntimeException(
                'AWS external cleanup failed: '.$failures[0]->getMessage(),
                previous: $failures[0],
            );
        }
    }

    /**
     * @return list<string>
     */
    private function externalSnsTopicArns(): array
    {
        $topicArns = [];

        foreach ($this->externalSns->getPaginator('ListTopics') as $result) {
            foreach ($result->get('Topics') ?? [] as $topic) {
                $topicArn = is_array($topic) ? ($topic['TopicArn'] ?? null) : null;
                $name = is_string($topicArn) ? strrchr($topicArn, ':') : false;

                if (
                    is_string($topicArn)
                    && is_string($name)
                    && str_starts_with(substr($name, 1), $this->externalResourceStem)
                ) {
                    $topicArns[] = $topicArn;
                }
            }
        }

        return $topicArns;
    }

    /**
     * @return list<string>
     */
    private function externalSqsQueueUrls(): array
    {
        $queueUrls = [];

        foreach ($this->externalSqs->getPaginator('ListQueues', [
            'QueueNamePrefix' => $this->externalResourceStem,
        ]) as $result) {
            foreach ($result->get('QueueUrls') ?? [] as $queueUrl) {
                $path = is_string($queueUrl) ? parse_url($queueUrl, PHP_URL_PATH) : null;
                $name = is_string($path) ? basename($path) : null;

                if (
                    is_string($queueUrl)
                    && is_string($name)
                    && str_starts_with($name, $this->externalResourceStem)
                ) {
                    $queueUrls[] = $queueUrl;
                }
            }
        }

        return $queueUrls;
    }
}
