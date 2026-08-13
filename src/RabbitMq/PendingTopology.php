<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\RabbitMqManagementException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;

class PendingTopology implements TopologyPlan
{
    /**
     * @param  list<string>  $exchanges
     * @param  list<array{name: string, arguments: array<string, int|string>}>  $queues
     * @param  list<array{exchange: string, queue: string}>  $bindings
     */
    public function __construct(
        private ManagementClient $managementClient,
        private readonly array $exchanges,
        private readonly array $queues,
        private readonly array $bindings,
    ) {}

    public function apply(): void
    {
        try {
            foreach ($this->exchanges as $exchange) {
                $this->managementClient->declareExchange($exchange);
            }

            foreach ($this->queues as $queue) {
                $this->managementClient->declareQueue($queue['name'], $queue['arguments']);
            }

            foreach ($this->bindings as $binding) {
                $this->managementClient->bindQueue($binding['exchange'], $binding['queue']);
            }
        } catch (RabbitMqManagementException $exception) {
            if ($exception->shouldRetry()) {
                throw TopologySyncRequiresRetryException::afterFailure($exception);
            }

            throw $exception;
        }
    }
}
