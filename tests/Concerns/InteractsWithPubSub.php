<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Concerns;

use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use Spoolrail\Spoolrail\PubSub\ConnectionConfig;
use Spoolrail\Spoolrail\SpoolrailManager;

trait InteractsWithPubSub
{
    private string|false $previousPubSubEmulatorHost;

    private string $pubSubProjectId;

    private PubSubClient $pubsub;

    protected function setUpInteractsWithPubSub(): void
    {
        $this->previousPubSubEmulatorHost = getenv('PUBSUB_EMULATOR_HOST');
        putenv('PUBSUB_EMULATOR_HOST=127.0.0.1:8085');

        $this->pubSubProjectId = 'spoolrail-'.bin2hex(random_bytes(6));

        config()->set('spoolrail.connections.pubsub', [
            'driver' => 'pubsub',
            'project_id' => $this->pubSubProjectId,
            'credentials' => null,
            'endpoint' => null,
            'message_ordering' => true,
            'exactly_once' => true,
        ]);

        $this->pubsub = new PubSubClient(
            new ConnectionConfig(
                'pubsub',
                config('spoolrail.connections.pubsub'),
            )->clientOptions(),
        );
    }

    protected function tearDownInteractsWithPubSub(): void
    {
        app(SpoolrailManager::class)->forgetConnection('pubsub');

        foreach ($this->pubsub->subscriptions() as $subscription) {
            $subscription->delete();
        }

        foreach ($this->pubsub->topics() as $topic) {
            $topic->delete();
        }

        if ($this->previousPubSubEmulatorHost === false) {
            putenv('PUBSUB_EMULATOR_HOST');
        } else {
            putenv("PUBSUB_EMULATOR_HOST={$this->previousPubSubEmulatorHost}");
        }
    }

    protected function pubSubTopic(string $topic): Topic
    {
        return $this->pubsub->topic($topic);
    }

    protected function pubSubSubscription(
        string $subscription,
        ?string $ownershipPrefix = null,
    ): Subscription {
        $prefix = $ownershipPrefix ?? config('spoolrail.prefix');

        return $this->pubsub->subscription("$prefix-$subscription");
    }
}
