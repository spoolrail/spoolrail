<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Concerns;

use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Illuminate\Support\Str;
use RuntimeException;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig;
use Spoolrail\Spoolrail\SpoolrailManager;
use Throwable;

trait InteractsWithExternalPubSub
{
    use InteractsWithExternalServices;

    private PubSubClient $externalPubSub;

    protected function setUpInteractsWithExternalPubSub(): void
    {
        $this->setUpExternalTestEnvironment();

        config()->set('spoolrail.connections.pubsub', $this->externalPubSubConnection());

        $config = new ConnectionConfig(
            'pubsub',
            config('spoolrail.connections.pubsub'),
        );
        $this->externalPubSub = new PubSubClient($config->singleAttemptClientOptions());

        $this->deleteExternalPubSubResources();
    }

    protected function tearDownInteractsWithExternalPubSub(): void
    {
        app(SpoolrailManager::class)->forgetConnection('pubsub');

        $this->deleteExternalPubSubResources();
    }

    protected function externalPubSubSubscription(): Subscription
    {
        return $this->externalPubSub->subscription(
            "$this->externalPrefix-$this->externalSubscription",
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function externalPubSubConnection(): array
    {
        return [
            'driver' => 'pubsub',
            'project_id' => $this->requiredExternalEnvironment('GOOGLE_CLOUD_PROJECT'),
            'credentials' => $this->requiredExternalEnvironment('SPOOLRAIL_GOOGLE_CREDENTIALS'),
            'endpoint' => $this->externalPubSubEndpoint(),
            'message_ordering' => true,
            'exactly_once' => true,
        ];
    }

    private function externalPubSubEndpoint(): string
    {
        $endpoint = $this->requiredExternalEnvironment('SPOOLRAIL_GOOGLE_PUBSUB_ENDPOINT');

        if (preg_match('/\A[a-z0-9-]+-pubsub\.googleapis\.com\z/', (string) $endpoint) !== 1) {
            throw new RuntimeException(
                'External tests require SPOOLRAIL_GOOGLE_PUBSUB_ENDPOINT to be a Google Pub/Sub production locational endpoint such as [us-east1-pubsub.googleapis.com].',
            );
        }

        return $endpoint;
    }

    private function deleteExternalPubSubResources(): void
    {
        $failures = [];
        $subscriptions = [];

        try {
            foreach ($this->externalPubSub->subscriptions() as $subscription) {
                $name = Str::afterLast((string) $subscription->name(), '/');

                if (str_starts_with($name, $this->externalResourceStem)) {
                    $subscriptions[] = $subscription;
                }
            }
        } catch (Throwable $exception) {
            $failures[] = $exception;
        }

        foreach ($subscriptions as $subscription) {
            try {
                $subscription->delete();
            } catch (Throwable $exception) {
                $failures[] = $exception;
            }
        }

        $topics = [];

        try {
            foreach ($this->externalPubSub->topics() as $topic) {
                $name = Str::afterLast((string) $topic->name(), '/');

                if (str_starts_with($name, $this->externalResourceStem)) {
                    $topics[] = $topic;
                }
            }
        } catch (Throwable $exception) {
            $failures[] = $exception;
        }

        foreach ($topics as $topic) {
            try {
                $topic->delete();
            } catch (Throwable $exception) {
                $failures[] = $exception;
            }
        }

        if ($failures !== []) {
            throw new RuntimeException(
                'Google Pub/Sub external cleanup failed: '.$failures[0]->getMessage(),
                previous: $failures[0],
            );
        }
    }
}
