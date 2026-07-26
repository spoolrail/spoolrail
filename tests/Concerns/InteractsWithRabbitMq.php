<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Concerns;

use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\PendingRequest;
use Illuminate\Http\Client\RequestException;
use Illuminate\Support\Facades\Http;
use Spoolrail\Spoolrail\SpoolrailManager;
use Throwable;

trait InteractsWithRabbitMq
{
    private string $rabbitMqVhost;

    private PendingRequest $rabbitMqManagement;

    protected function setUpInteractsWithRabbitMq(): void
    {
        $this->rabbitMqVhost = $this->isolatedVhostName();
        $this->rabbitMqManagement = $this->managementApi();

        $this->createIsolatedVhost();
        $this->pointSpoolrailAtIsolatedVhost();
    }

    protected function tearDownInteractsWithRabbitMq(): void
    {
        $this->disconnectSpoolrail();
        $this->deleteIsolatedVhost();
    }

    protected function useQuorumQueues(): void
    {
        $this->rabbitMqManagement
            ->put("/api/vhosts/$this->rabbitMqVhost", [
                'default_queue_type' => 'quorum',
            ])
            ->throw();
    }

    /**
     * @return array<string, mixed>
     *
     * @throws ConnectionException
     * @throws RequestException
     */
    protected function rabbitMqQueue(string $queue): array
    {
        return $this->rabbitMqManagement
            ->get("/api/queues/$this->rabbitMqVhost/$queue")
            ->throw()
            ->json();
    }

    /**
     * @return list<array<string, mixed>>
     *
     * @throws ConnectionException
     * @throws RequestException
     */
    protected function drainRabbitMqDeliveries(string $queue, int $count): array
    {
        return $this->rabbitMqManagement
            ->post(
                "/api/queues/$this->rabbitMqVhost/$queue/get",
                [
                    'count' => $count,
                    'ackmode' => 'ack_requeue_false',
                    'encoding' => 'auto',
                ],
            )
            ->throw()
            ->json();
    }

    private function isolatedVhostName(): string
    {
        return 'spoolrail-test-'.bin2hex(random_bytes(6));
    }

    private function managementApi(): PendingRequest
    {
        return Http::baseUrl(
            (string) config('spoolrail.connections.rabbitmq.management.url'),
        )->withBasicAuth(
            (string) config('spoolrail.connections.rabbitmq.management.username'),
            (string) config('spoolrail.connections.rabbitmq.management.password'),
        );
    }

    private function createIsolatedVhost(): void
    {
        $this->rabbitMqManagement
            ->send('PUT', "/api/vhosts/$this->rabbitMqVhost")
            ->throw();

        try {
            $this->grantSpoolrailAccessToIsolatedVhost();
        } catch (Throwable $exception) {
            $this->rabbitMqManagement->delete("/api/vhosts/$this->rabbitMqVhost");

            throw $exception;
        }
    }

    private function grantSpoolrailAccessToIsolatedVhost(): void
    {
        $username = rawurlencode(
            (string) config('spoolrail.connections.rabbitmq.username'),
        );

        $this->rabbitMqManagement
            ->put("/api/permissions/$this->rabbitMqVhost/$username", [
                'configure' => '.*',
                'write' => '.*',
                'read' => '.*',
            ])
            ->throw();
    }

    private function pointSpoolrailAtIsolatedVhost(): void
    {
        config()->set('spoolrail.connections.rabbitmq.vhost', $this->rabbitMqVhost);
    }

    private function disconnectSpoolrail(): void
    {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
    }

    private function deleteIsolatedVhost(): void
    {
        $this->rabbitMqManagement
            ->delete("/api/vhosts/$this->rabbitMqVhost")
            ->throw();
    }
}
