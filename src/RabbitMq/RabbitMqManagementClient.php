<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Illuminate\Http\Client\Factory;
use Illuminate\Http\Client\PendingRequest;
use Illuminate\Http\Client\Response;
use Spoolrail\Spoolrail\Exceptions\RabbitMqManagementException;
use Throwable;

class RabbitMqManagementClient
{
    private const int PAGE_SIZE = 500;

    private ?RabbitMqManagementConfig $management = null;

    public function __construct(
        private readonly RabbitMqConnectionConfig $connection,
        private readonly Factory $http,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function overview(): array
    {
        return $this->object('overview', 'reading the broker version');
    }

    /**
     * @return array<string, mixed>
     */
    public function virtualHost(): array
    {
        return $this->object(
            'vhosts/'.$this->segment($this->connection->virtualHost()),
            'reading the virtual host',
        );
    }

    /**
     * @return array<string, mixed>|null
     */
    public function exchange(string $exchange): ?array
    {
        return $this->optionalObject(
            'exchanges/'.$this->virtualHostSegment().'/'.$this->segment($exchange),
            "reading exchange [$exchange]",
        );
    }

    /**
     * @return array<string, mixed>|null
     */
    public function queue(string $queue): ?array
    {
        return $this->optionalObject(
            'queues/'.$this->virtualHostSegment().'/'.$this->segment($queue),
            "reading queue [$queue]",
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function queuesOwnedBy(string $ownershipPrefix): array
    {
        $operation = "listing queues owned by prefix [$ownershipPrefix]";
        $queues = [];
        $page = 1;

        do {
            $response = $this->request(
                'GET',
                'queues/'.$this->virtualHostSegment(),
                $operation,
                query: [
                    'name' => '^'.preg_quote("$ownershipPrefix-", '/'),
                    'use_regex' => 'true',
                    'pagination' => 'true',
                    'page' => (string) $page,
                    'page_size' => (string) self::PAGE_SIZE,
                    'disable_stats' => 'true',
                ],
            );
            [$items, $pageCount] = $this->decodedPage($response, $operation, $page);
            array_push($queues, ...$items);
            $page++;
        } while ($page <= $pageCount);

        return $queues;
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function queueBindings(string $queue): array
    {
        return $this->objects(
            'queues/'.$this->virtualHostSegment().'/'.$this->segment($queue).'/bindings',
            "reading bindings for queue [$queue]",
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function policies(): array
    {
        return $this->objects(
            'policies/'.$this->virtualHostSegment(),
            'reading queue policies',
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function operatorPolicies(): array
    {
        return $this->objects(
            'operator-policies/'.$this->virtualHostSegment(),
            'reading operator policies',
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function exchangeSourceBindings(string $exchange): array
    {
        return $this->objects(
            'exchanges/'.$this->virtualHostSegment().'/'.$this->segment($exchange).'/bindings/source',
            "reading outgoing bindings for topic [$exchange]",
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function exchangeDestinationBindings(string $exchange): array
    {
        return $this->objects(
            'exchanges/'.$this->virtualHostSegment().'/'.$this->segment($exchange).'/bindings/destination',
            "reading incoming bindings for topic [$exchange]",
        );
    }

    public function declareExchange(string $exchange): void
    {
        $this->mutation(
            'PUT',
            'exchanges/'.$this->virtualHostSegment().'/'.$this->segment($exchange),
            [
                'type' => 'fanout',
                'durable' => true,
                'auto_delete' => false,
                'internal' => false,
                'arguments' => (object) [],
            ],
            "creating exchange [$exchange]",
        );
    }

    /**
     * @param  array<string, int|string>  $arguments
     */
    public function declareQueue(string $queue, array $arguments): void
    {
        $this->mutation(
            'PUT',
            'queues/'.$this->virtualHostSegment().'/'.$this->segment($queue),
            [
                'durable' => true,
                'auto_delete' => false,
                'arguments' => $arguments === [] ? (object) [] : $arguments,
            ],
            "creating queue [$queue]",
        );
    }

    public function bindQueue(string $exchange, string $queue): void
    {
        $this->mutation(
            'POST',
            'bindings/'.$this->virtualHostSegment()
                .'/e/'.$this->segment($exchange)
                .'/q/'.$this->segment($queue),
            [
                'routing_key' => '',
                'arguments' => (object) [],
            ],
            "binding exchange [$exchange] to queue [$queue]",
        );
    }

    public function deleteQueue(string $queue): void
    {
        $this->mutation(
            'DELETE',
            'queues/'.$this->virtualHostSegment().'/'.$this->segment($queue),
            [],
            "deleting queue [$queue]",
            allowNotFound: true,
        );
    }

    public function deleteExchangeIfUnused(string $exchange): void
    {
        $this->mutation(
            'DELETE',
            'exchanges/'.$this->virtualHostSegment().'/'.$this->segment($exchange),
            [],
            "deleting topic [$exchange]",
            query: ['if-unused' => 'true'],
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function object(string $path, string $operation): array
    {
        return $this->decodedObject(
            $this->request('GET', $path, $operation),
            $operation,
        );
    }

    /**
     * @return array<string, mixed>|null
     */
    private function optionalObject(string $path, string $operation): ?array
    {
        $response = $this->request('GET', $path, $operation, allowNotFound: true);

        if ($response->notFound()) {
            return null;
        }

        return $this->decodedObject($response, $operation);
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function objects(string $path, string $operation): array
    {
        $response = $this->request('GET', $path, $operation);
        $decoded = $response->json();

        if (! is_array($decoded) || ! array_is_list($decoded)) {
            throw RabbitMqManagementException::invalidResponse(
                $this->connection->connection,
                $operation,
            );
        }

        foreach ($decoded as $value) {
            if (! is_array($value) || array_is_list($value)) {
                throw RabbitMqManagementException::invalidResponse(
                    $this->connection->connection,
                    $operation,
                );
            }
        }

        /** @var list<array<string, mixed>> $decoded */
        return $decoded;
    }

    /**
     * @return array<string, mixed>
     */
    private function decodedObject(Response $response, string $operation): array
    {
        $decoded = $response->json();

        if (! is_array($decoded) || array_is_list($decoded)) {
            throw RabbitMqManagementException::invalidResponse(
                $this->connection->connection,
                $operation,
            );
        }

        $object = [];

        foreach ($decoded as $key => $value) {
            if (! is_string($key)) {
                throw RabbitMqManagementException::invalidResponse(
                    $this->connection->connection,
                    $operation,
                );
            }

            $object[$key] = $value;
        }

        return $object;
    }

    /**
     * @return array{list<array<string, mixed>>, int}
     */
    private function decodedPage(Response $response, string $operation, int $requestedPage): array
    {
        $decoded = $response->json();

        if (
            ! is_array($decoded)
            || array_is_list($decoded)
            || ($decoded['page'] ?? null) !== $requestedPage
            || ! is_int($decoded['page_count'] ?? null)
            || $decoded['page_count'] < 0
            || ! is_array($decoded['items'] ?? null)
            || ! array_is_list($decoded['items'])
            || ($decoded['page_count'] === 0
                ? $requestedPage !== 1
                : $requestedPage > $decoded['page_count'])
        ) {
            throw RabbitMqManagementException::invalidResponse(
                $this->connection->connection,
                $operation,
            );
        }

        foreach ($decoded['items'] as $item) {
            if (! is_array($item) || array_is_list($item)) {
                throw RabbitMqManagementException::invalidResponse(
                    $this->connection->connection,
                    $operation,
                );
            }
        }

        /** @var list<array<string, mixed>> $items */
        $items = $decoded['items'];

        return [$items, $decoded['page_count']];
    }

    /**
     * @param  array<string, mixed>  $payload
     * @param  array<string, string>  $query
     */
    private function mutation(
        string $method,
        string $path,
        array $payload,
        string $operation,
        bool $allowNotFound = false,
        array $query = [],
    ): void {
        $this->request(
            $method,
            $path,
            $operation,
            $payload,
            $allowNotFound,
            $query,
        );
    }

    /**
     * @param  array<string, mixed>  $payload
     * @param  array<string, string>  $query
     */
    private function request(
        string $method,
        string $path,
        string $operation,
        array $payload = [],
        bool $allowNotFound = false,
        array $query = [],
    ): Response {
        $options = [];

        if ($payload !== []) {
            $options['json'] = $payload;
        }

        if ($query !== []) {
            $options['query'] = $query;
        }

        $pending = $this->pendingRequest();

        try {
            $response = $pending->send($method, $path, $options);
        } catch (Throwable $exception) {
            throw RabbitMqManagementException::requestFailed(
                $this->connection->connection,
                $operation,
                $exception->getMessage(),
            );
        }

        if (! $response->successful() && (! $allowNotFound || ! $response->notFound())) {
            throw RabbitMqManagementException::unexpectedStatus(
                $this->connection->connection,
                $operation,
                $response->status(),
            );
        }

        return $response;
    }

    /**
     * @internal
     */
    public function pendingRequest(): PendingRequest
    {
        $management = $this->management();

        return $this->http
            ->baseUrl($this->apiUrl())
            ->withBasicAuth($management->username, $management->password)
            ->acceptJson()
            ->asJson()
            ->withOptions([
                'verify' => $management->caFile ?? true,
            ]);
    }

    private function apiUrl(): string
    {
        $url = $this->management()->url;

        if (str_ends_with(parse_url($url, PHP_URL_PATH) ?: '', '/api')) {
            return $url;
        }

        return $url.'/api';
    }

    private function virtualHostSegment(): string
    {
        return $this->segment($this->connection->virtualHost());
    }

    private function segment(string $value): string
    {
        return rawurlencode($value);
    }

    private function management(): RabbitMqManagementConfig
    {
        return $this->management ??= $this->connection->management();
    }
}
