<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Spoolrail\Spoolrail\Contracts\TopologyPlan;

readonly class RabbitMqTopologyPlan implements TopologyPlan
{
    /**
     * @param  list<string>  $exchanges
     * @param  list<array{name: string, arguments: array<string, int|string>}>  $queues
     * @param  list<array{exchange: string, queue: string}>  $bindings
     */
    public function __construct(
        private RabbitMqManagementClient $management,
        private array $exchanges,
        private array $queues,
        private array $bindings,
    ) {}

    public function apply(): void
    {
        foreach ($this->exchanges as $exchange) {
            $this->management->declareExchange($exchange);
        }

        foreach ($this->queues as $queue) {
            $this->management->declareQueue($queue['name'], $queue['arguments']);
        }

        foreach ($this->bindings as $binding) {
            $this->management->bindQueue($binding['exchange'], $binding['queue']);
        }
    }
}
