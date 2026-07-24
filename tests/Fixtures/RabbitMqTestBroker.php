<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use RuntimeException;

class RabbitMqTestBroker
{
    public readonly string $prefix;

    private function __construct(
        public readonly string $virtualHost,
    ) {
        $this->prefix = 'test-'.bin2hex(random_bytes(6));
    }

    public static function create(string $defaultQueueType = 'classic'): self
    {
        $broker = new self('spoolrail-test-'.bin2hex(random_bytes(6)));

        $broker->request(
            'PUT',
            '/api/vhosts/'.rawurlencode($broker->virtualHost),
            [
                'description' => 'Isolated Spoolrail package test',
                'tags' => [],
                'default_queue_type' => $defaultQueueType,
                'tracing' => false,
            ],
            [201, 204],
        );
        $broker->request(
            'PUT',
            '/api/permissions/'.rawurlencode($broker->virtualHost).'/'.rawurlencode(self::username()),
            [
                'configure' => '.*',
                'write' => '.*',
                'read' => '.*',
            ],
            [201, 204],
        );

        return $broker;
    }

    /**
     * @return array<string, mixed>
     */
    public function connectionConfiguration(): array
    {
        return [
            'driver' => 'rabbitmq',
            'url' => $this->amqpUrl().'/'.rawurlencode($this->virtualHost),
            'management' => [
                'url' => $this->managementUrl(),
                'username' => self::username(),
                'password' => $this->password(),
            ],
        ];
    }

    public function configureApplication(string $connection = 'rabbitmq'): void
    {
        config()->set('spoolrail.default', $connection);
        config()->set('spoolrail.prefix', $this->prefix);
        config()->set("spoolrail.connections.$connection", $this->connectionConfiguration());
    }

    public function configureDataPlaneApplication(string $connection = 'rabbitmq'): void
    {
        $configuration = $this->connectionConfiguration();
        unset($configuration['management']);

        config()->set('spoolrail.default', $connection);
        config()->set('spoolrail.prefix', $this->prefix);
        config()->set("spoolrail.connections.$connection", $configuration);
    }

    public function queueName(string $subscription): string
    {
        return "{$this->prefix}-$subscription";
    }

    public function delete(): void
    {
        $this->request(
            'DELETE',
            '/api/vhosts/'.rawurlencode($this->virtualHost),
            null,
            [204, 404],
        );
    }

    /**
     * @return array<string, mixed>|null
     */
    public function exchange(string $name): ?array
    {
        return $this->object(
            '/api/exchanges/'.$this->virtualHostSegment().'/'.rawurlencode($name),
        );
    }

    /**
     * @return array<string, mixed>|null
     */
    public function queue(string $name): ?array
    {
        return $this->object(
            '/api/queues/'.$this->virtualHostSegment().'/'.rawurlencode($name),
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function queueBindings(string $queue): array
    {
        $bindings = $this->request(
            'GET',
            '/api/queues/'.$this->virtualHostSegment().'/'.rawurlencode($queue).'/bindings',
            null,
            [200],
        );

        if (! is_array($bindings) || ! array_is_list($bindings)) {
            throw new RuntimeException('RabbitMQ returned invalid queue bindings JSON.');
        }

        /** @var list<array<string, mixed>> $bindings */
        return $bindings;
    }

    public function declareExchange(string $name, string $type = 'fanout'): void
    {
        $this->request(
            'PUT',
            '/api/exchanges/'.$this->virtualHostSegment().'/'.rawurlencode($name),
            [
                'type' => $type,
                'durable' => true,
                'auto_delete' => false,
                'internal' => false,
                'arguments' => (object) [],
            ],
            [201, 204],
        );
    }

    /**
     * @param  array<string, int|string>  $arguments
     */
    public function declareQueue(string $name, array $arguments = []): void
    {
        $this->request(
            'PUT',
            '/api/queues/'.$this->virtualHostSegment().'/'.rawurlencode($name),
            [
                'durable' => true,
                'auto_delete' => false,
                'arguments' => $arguments === [] ? (object) [] : $arguments,
            ],
            [201, 204],
        );
    }

    public function bind(string $exchange, string $queue): void
    {
        $this->request(
            'POST',
            '/api/bindings/'.$this->virtualHostSegment()
                .'/e/'.rawurlencode($exchange)
                .'/q/'.rawurlencode($queue),
            [
                'routing_key' => '',
                'arguments' => (object) [],
            ],
            [201],
        );
    }

    public function message(string $queue): ?string
    {
        $messages = $this->request(
            'POST',
            '/api/queues/'.$this->virtualHostSegment().'/'.rawurlencode($queue).'/get',
            [
                'count' => 1,
                'ackmode' => 'ack_requeue_false',
                'encoding' => 'auto',
                'truncate' => 300_000,
            ],
            [200],
        );

        if (! is_array($messages) || ! array_is_list($messages)) {
            throw new RuntimeException('RabbitMQ returned invalid message JSON.');
        }

        if ($messages === []) {
            return null;
        }

        $payload = $messages[0]['payload'] ?? null;

        if (! is_string($payload)) {
            throw new RuntimeException('RabbitMQ returned a message without a string payload.');
        }

        return $payload;
    }

    /**
     * @return array<string, mixed>|null
     */
    private function object(string $path): ?array
    {
        $result = $this->request('GET', $path, null, [200, 404], returnStatus: true);

        if ($result['status'] === 404) {
            return null;
        }

        $body = $result['body'];

        if (! is_array($body) || array_is_list($body)) {
            throw new RuntimeException('RabbitMQ returned invalid object JSON.');
        }

        /** @var array<string, mixed> $body */
        return $body;
    }

    private function virtualHostSegment(): string
    {
        return rawurlencode($this->virtualHost);
    }

    /**
     * @param  array<string, mixed>|null  $payload
     * @param  list<int>  $expectedStatuses
     */
    private function request(
        string $method,
        string $path,
        ?array $payload,
        array $expectedStatuses,
        bool $returnStatus = false,
    ): mixed {
        $handle = curl_init($this->managementUrl().$path);

        if ($handle === false) {
            throw new RuntimeException('Unable to initialize cURL.');
        }

        $headers = ['Accept: application/json'];
        $options = [
            CURLOPT_CUSTOMREQUEST => $method,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CONNECTTIMEOUT => 3,
            CURLOPT_TIMEOUT => 15,
            CURLOPT_HTTPAUTH => CURLAUTH_BASIC,
            CURLOPT_USERPWD => self::username().':'.$this->password(),
        ];

        if ($payload !== null) {
            $options[CURLOPT_POSTFIELDS] = json_encode($payload, JSON_THROW_ON_ERROR);
            $headers[] = 'Content-Type: application/json';
        }

        $options[CURLOPT_HTTPHEADER] = $headers;
        curl_setopt_array($handle, $options);

        $response = curl_exec($handle);
        $status = curl_getinfo($handle, CURLINFO_RESPONSE_CODE);
        $failure = curl_error($handle);
        curl_close($handle);

        if ($response === false) {
            throw new RuntimeException(
                "RabbitMQ test dependency is unavailable; start it with [docker compose up -d --wait rabbitmq]. Management request failed: $failure",
            );
        }

        if (! in_array($status, $expectedStatuses, true)) {
            throw new RuntimeException(
                "RabbitMQ Management request [$method $path] returned HTTP $status: $response",
            );
        }

        $body = $response === '' ? null : json_decode($response, true, flags: JSON_THROW_ON_ERROR);

        return $returnStatus ? ['status' => $status, 'body' => $body] : $body;
    }

    private function amqpUrl(): string
    {
        return self::environment(
            'SPOOLRAIL_TEST_AMQP_URL',
            'amqp://spoolrail:spoolrail@127.0.0.1:5672',
        );
    }

    private function managementUrl(): string
    {
        return self::environment(
            'SPOOLRAIL_TEST_MANAGEMENT_URL',
            'http://127.0.0.1:15672',
        );
    }

    private static function username(): string
    {
        return self::environment('SPOOLRAIL_TEST_RABBITMQ_USERNAME', 'spoolrail');
    }

    private function password(): string
    {
        return self::environment('SPOOLRAIL_TEST_RABBITMQ_PASSWORD', 'spoolrail');
    }

    private static function environment(string $name, string $default): string
    {
        $value = getenv($name);

        return is_string($value) && $value !== '' ? $value : $default;
    }
}
