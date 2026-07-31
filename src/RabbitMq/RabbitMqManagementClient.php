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

    private ?RabbitMqManagementConfig $managementConfig = null;

    public function __construct(
        private readonly RabbitMqConnectionConfig $config,
        private readonly Factory $http,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function overview(): array
    {
        return $this->read('overview', 'reading the broker version');
    }

    /**
     * @return array<string, mixed>
     */
    public function virtualHost(): array
    {
        return $this->read(
            'vhosts/'.$this->segment($this->config->virtualHost()),
            'reading the virtual host',
        );
    }

    /**
     * @return array<string, mixed>|null
     */
    public function exchange(string $exchange): ?array
    {
        return $this->find(
            'exchanges/'.$this->virtualHostSegment().'/'.$this->segment($exchange),
            "reading exchange [$exchange]",
        );
    }

    /**
     * @return array<string, mixed>|null
     */
    public function queue(string $queue): ?array
    {
        return $this->find(
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
            [$items, $pageCount] = $this->decodePage($response, $operation, $page);
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
        return $this->readAll(
            'queues/'.$this->virtualHostSegment().'/'.$this->segment($queue).'/bindings',
            "reading bindings for queue [$queue]",
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function policies(): array
    {
        return $this->readAll(
            'policies/'.$this->virtualHostSegment(),
            'reading queue policies',
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function operatorPolicies(): array
    {
        return $this->readAll(
            'operator-policies/'.$this->virtualHostSegment(),
            'reading operator policies',
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function exchangeSourceBindings(string $exchange): array
    {
        return $this->readAll(
            'exchanges/'.$this->virtualHostSegment().'/'.$this->segment($exchange).'/bindings/source',
            "reading outgoing bindings for topic [$exchange]",
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function exchangeDestinationBindings(string $exchange): array
    {
        return $this->readAll(
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
    private function read(string $path, string $operation): array
    {
        return $this->decodeMap(
            $this->request('GET', $path, $operation),
            $operation,
        );
    }

    /**
     * @return array<string, mixed>|null
     */
    private function find(string $path, string $operation): ?array
    {
        $response = $this->request('GET', $path, $operation, allowNotFound: true);

        if ($response->notFound()) {
            return null;
        }

        return $this->decodeMap($response, $operation);
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function readAll(string $path, string $operation): array
    {
        return $this->requireListOfMaps(
            $this->request('GET', $path, $operation)->json(),
            $operation,
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function decodeMap(Response $response, string $operation): array
    {
        return $this->requireMap($response->json(), $operation);
    }

    /**
     * @return array{list<array<string, mixed>>, int}
     */
    private function decodePage(Response $response, string $operation, int $requestedPage): array
    {
        $page = $this->requireMap($response->json(), $operation);

        if (($page['page'] ?? null) !== $requestedPage) {
            $this->rejectInvalidResponse($operation);
        }

        $pageCount = $this->requireNonNegativeInteger($page['page_count'] ?? null, $operation);

        if ($requestedPage > max(1, $pageCount)) {
            $this->rejectInvalidResponse($operation);
        }

        $items = $this->requireListOfMaps($page['items'] ?? null, $operation);

        return [$items, $pageCount];
    }

    /**
     * @return array<string, mixed>
     */
    private function requireMap(mixed $decoded, string $operation): array
    {
        if (! is_array($decoded) || array_is_list($decoded)) {
            $this->rejectInvalidResponse($operation);
        }

        $keys = array_keys($decoded);

        if (array_filter($keys, is_string(...)) !== $keys) {
            $this->rejectInvalidResponse($operation);
        }

        /** @var array<string, mixed> $decoded */
        return $decoded;
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function requireListOfMaps(mixed $decoded, string $operation): array
    {
        if (! is_array($decoded) || ! array_is_list($decoded)) {
            $this->rejectInvalidResponse($operation);
        }

        return array_map(
            fn (mixed $map): array => $this->requireMap($map, $operation),
            $decoded,
        );
    }

    private function requireNonNegativeInteger(mixed $value, string $operation): int
    {
        if (! is_int($value) || $value < 0) {
            $this->rejectInvalidResponse($operation);
        }

        return $value;
    }

    private function rejectInvalidResponse(string $operation): never
    {
        throw RabbitMqManagementException::invalidResponse(
            $this->config->connectionName,
            $operation,
        );
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
        $options = array_filter([
            'json' => $payload,
            'query' => $query,
        ]);

        $pending = $this->pendingRequest();

        try {
            $response = $pending->send($method, $path, $options);
        } catch (Throwable $exception) {
            throw RabbitMqManagementException::requestFailed(
                $this->config->connectionName,
                $operation,
                $exception,
            );
        }

        if ($response->successful() || ($allowNotFound && $response->notFound())) {
            return $response;
        }

        throw RabbitMqManagementException::unexpectedStatus(
            $this->config->connectionName,
            $operation,
            $response->status(),
        );
    }

    /**
     * @internal
     */
    public function pendingRequest(): PendingRequest
    {
        $managementConfig = $this->managementConfig();

        /** @var PendingRequest $pending */
        $pending = $this->http->baseUrl($this->apiUrl());

        return $pending
            ->withBasicAuth($managementConfig->username, $managementConfig->password)
            ->acceptJson()
            ->asJson()
            ->withOptions([
                'verify' => $managementConfig->caFile ?? true,
            ]);
    }

    private function apiUrl(): string
    {
        $url = $this->managementConfig()->url;

        if (str_ends_with(parse_url($url, PHP_URL_PATH) ?: '', '/api')) {
            return $url;
        }

        return $url.'/api';
    }

    private function virtualHostSegment(): string
    {
        return $this->segment($this->config->virtualHost());
    }

    private function segment(string $value): string
    {
        return rawurlencode($value);
    }

    private function managementConfig(): RabbitMqManagementConfig
    {
        return $this->managementConfig ??= $this->config->management();
    }
}
