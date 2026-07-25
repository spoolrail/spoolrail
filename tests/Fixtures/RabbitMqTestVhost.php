<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Facades\Http;
use Spoolrail\Spoolrail\SpoolrailManager;

class RabbitMqTestVhost
{
    private function __construct(
        public readonly string $name,
        public readonly PendingRequest $management,
    ) {}

    public static function create(): self
    {
        $name = 'spoolrail-test-'.bin2hex(random_bytes(6));
        $username = (string) config('spoolrail.connections.rabbitmq.username');
        $password = (string) config('spoolrail.connections.rabbitmq.password');
        $management = Http::baseUrl(
            (string) config('spoolrail.connections.rabbitmq.management.url'),
        )->withBasicAuth(
            (string) (config('spoolrail.connections.rabbitmq.management.username') ?? $username),
            (string) (config('spoolrail.connections.rabbitmq.management.password') ?? $password),
        )->acceptJson();

        $encodedName = rawurlencode($name);
        $encodedUsername = rawurlencode($username);

        $management->put("/api/vhosts/$encodedName", [
            'description' => 'Isolated Spoolrail package test',
            'tags' => [],
            'default_queue_type' => 'quorum',
            'tracing' => false,
        ])->throw();
        $management->put("/api/permissions/$encodedName/$encodedUsername", [
            'configure' => '.*',
            'write' => '.*',
            'read' => '.*',
        ])->throw();

        config()->set('spoolrail.connections.rabbitmq.vhost', $name);

        return new self($name, $management);
    }

    public function delete(): void
    {
        app(SpoolrailManager::class)->forgetConnection('rabbitmq');
        $this->management->delete('/api/vhosts/'.rawurlencode($this->name))->throw();
    }
}
