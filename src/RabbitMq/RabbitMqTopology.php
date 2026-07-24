<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\RabbitMqManagementException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\Exceptions\UnsupportedRabbitMqVersionException;
use Spoolrail\Spoolrail\Subscriptions\Subscription;

class RabbitMqTopology implements ManagedTopology
{
    public function __construct(
        private readonly RabbitMqConnectionConfig $connection,
        private readonly RabbitMqManagementClient $management,
    ) {}

    /**
     * @param  list<Subscription>  $subscriptions
     */
    public function planSync(array $subscriptions, string $ownershipPrefix): TopologyPlan
    {
        $resources = array_map(
            static fn (Subscription $subscription): array => [
                'topic' => RabbitMqName::topic($subscription->topic()),
                'queue' => RabbitMqName::queue($ownershipPrefix, $subscription->name()),
            ],
            $subscriptions,
        );

        $this->assertSupportedVersion();

        $defaultQueueType = null;
        $policies = $this->management->policies();
        $operatorPolicies = $this->management->operatorPolicies();
        $missingExchanges = [];
        $missingQueues = [];
        $missingBindings = [];
        $inspectedExchanges = [];

        foreach ($resources as $resource) {
            $topic = $resource['topic'];
            $queueName = $resource['queue'];

            if (! isset($inspectedExchanges[$topic])) {
                if ($this->inspectExchange($topic)) {
                    $missingExchanges[] = $topic;
                }

                $inspectedExchanges[$topic] = true;
            }

            $queue = $this->management->queue($queueName);

            if ($queue === null) {
                $arguments = $this->missingQueueArguments(
                    $queueName,
                    $defaultQueueType ??= $this->defaultQueueType(),
                    $policies,
                    $operatorPolicies,
                );
                $missingQueues[] = [
                    'name' => $queueName,
                    'arguments' => $arguments,
                ];
                $missingBindings[] = [
                    'exchange' => $topic,
                    'queue' => $queueName,
                ];

                continue;
            }

            $this->assertCompatibleQueue(
                $queueName,
                $queue,
                $policies,
                $operatorPolicies,
            );

            if ($this->inspectBindings($queueName, $topic)) {
                $missingBindings[] = [
                    'exchange' => $topic,
                    'queue' => $queueName,
                ];
            }
        }

        return new RabbitMqTopologyPlan(
            $this->management,
            $missingExchanges,
            $missingQueues,
            $missingBindings,
        );
    }

    /**
     * @param  list<Subscription>  $subscriptions
     * @return list<string>
     */
    public function undeclaredSubscriptions(
        array $subscriptions,
        string $ownershipPrefix,
    ): array {
        $namespace = "$ownershipPrefix-";
        $declared = [];

        foreach ($subscriptions as $subscription) {
            $declared[RabbitMqName::queue($ownershipPrefix, $subscription->name())] = true;
        }

        $this->assertSupportedVersion();

        $undeclared = [];

        foreach ($this->management->queuesOwnedBy($ownershipPrefix) as $queue) {
            $name = $queue['name'] ?? null;

            if (
                is_string($name)
                && str_starts_with($name, $namespace)
                && ! isset($declared[$name])
            ) {
                $undeclared[] = $name;
            }
        }

        sort($undeclared);

        return $undeclared;
    }

    public function deleteSubscription(string $physicalName): void
    {
        $this->management->deleteQueue($physicalName);
    }

    public function deleteTopic(string $topic): void
    {
        RabbitMqName::topic($topic);
        $this->assertSupportedVersion();

        $exchange = $this->management->exchange($topic);

        if ($exchange === null) {
            throw RabbitMqTopologyException::topicMissing($topic);
        }

        $this->assertCompatibleExchange($topic, $exchange);

        if (
            $this->management->exchangeSourceBindings($topic) !== []
            || $this->management->exchangeDestinationBindings($topic) !== []
        ) {
            throw RabbitMqTopologyException::topicHasBindings($topic);
        }

        $this->management->deleteExchangeIfUnused($topic);
    }

    private function assertSupportedVersion(): void
    {
        $version = $this->management->overview()['rabbitmq_version'] ?? null;

        if (! is_string($version)) {
            throw RabbitMqManagementException::invalidResponse(
                $this->connection->connection,
                'reading the broker version',
            );
        }

        if (version_compare($version, RabbitMqVersion::MINIMUM, '<')) {
            throw new UnsupportedRabbitMqVersionException($version);
        }
    }

    private function defaultQueueType(): string
    {
        $type = $this->management->virtualHost()['default_queue_type'] ?? null;

        if (! is_string($type) || ! in_array($type, ['classic', 'quorum'], true)) {
            throw RabbitMqTopologyException::unsupportedDefaultQueueType(
                is_string($type) ? $type : 'unknown',
            );
        }

        return $type;
    }

    /**
     * @return bool Whether the exchange must be created.
     */
    private function inspectExchange(string $topic): bool
    {
        $exchange = $this->management->exchange($topic);

        if ($exchange === null) {
            return true;
        }

        $this->assertCompatibleExchange($topic, $exchange);

        return false;
    }

    /**
     * @param  array<string, mixed>  $exchange
     */
    private function assertCompatibleExchange(string $topic, array $exchange): void
    {
        $requirements = [
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ];

        foreach ($requirements as $setting => $required) {
            if (($exchange[$setting] ?? null) !== $required) {
                throw RabbitMqTopologyException::incompatibleExchange(
                    $topic,
                    "[$setting] must be ".var_export($required, true),
                );
            }
        }
    }

    /**
     * @param  list<array<string, mixed>>  $policies
     * @param  list<array<string, mixed>>  $operatorPolicies
     * @return array<string, int|string>
     */
    private function missingQueueArguments(
        string $queue,
        string $defaultQueueType,
        array $policies,
        array $operatorPolicies,
    ): array {
        if ($defaultQueueType === 'classic') {
            return [];
        }

        foreach ([$operatorPolicies, $policies] as $policySet) {
            $limit = $this->applicableDeliveryLimit(
                $queue,
                'quorum',
                $policySet,
            );

            if ($limit !== null && $limit !== -1) {
                throw RabbitMqTopologyException::finiteDeliveryLimit($queue, $limit);
            }
        }

        return ['x-delivery-limit' => -1];
    }

    /**
     * @param  array<string, mixed>  $queue
     * @param  list<array<string, mixed>>  $policies
     * @param  list<array<string, mixed>>  $operatorPolicies
     */
    private function assertCompatibleQueue(
        string $name,
        array $queue,
        array $policies,
        array $operatorPolicies,
    ): void {
        $requirements = [
            'durable' => true,
            'exclusive' => false,
            'auto_delete' => false,
        ];

        foreach ($requirements as $setting => $required) {
            if (($queue[$setting] ?? null) !== $required) {
                throw RabbitMqTopologyException::incompatibleQueue(
                    $name,
                    "[$setting] must be ".var_export($required, true),
                );
            }
        }

        $type = $queue['type'] ?? null;

        if (! is_string($type) || ! in_array($type, ['classic', 'quorum'], true)) {
            throw RabbitMqTopologyException::incompatibleQueue(
                $name,
                'queue type must be classic or quorum',
            );
        }

        if ($type === 'quorum') {
            $this->assertUnlimitedDeliveryLimit(
                $name,
                $queue,
                $policies,
                $operatorPolicies,
            );
        }
    }

    /**
     * @param  array<string, mixed>  $queue
     * @param  list<array<string, mixed>>  $policies
     * @param  list<array<string, mixed>>  $operatorPolicies
     */
    private function assertUnlimitedDeliveryLimit(
        string $name,
        array $queue,
        array $policies,
        array $operatorPolicies,
    ): void {
        $arguments = $queue['arguments'] ?? [];
        $declaredLimit = is_array($arguments)
            ? $this->integerValue($arguments['x-delivery-limit'] ?? null)
            : null;

        $limits = [
            $declaredLimit,
            $this->applicableDeliveryLimit($name, 'quorum', $policies),
            $this->applicableDeliveryLimit($name, 'quorum', $operatorPolicies),
        ];

        foreach ($limits as $limit) {
            if ($limit !== null && $limit !== -1) {
                throw RabbitMqTopologyException::finiteDeliveryLimit($name, $limit);
            }
        }

        if (in_array(-1, $limits, true)) {
            return;
        }

        throw RabbitMqTopologyException::indeterminateDeliveryLimit($name);
    }

    /**
     * @return bool Whether the required binding must be created.
     */
    private function inspectBindings(string $queue, string $topic): bool
    {
        $bindings = array_values(array_filter(
            $this->management->queueBindings($queue),
            static fn (array $binding): bool => ($binding['source'] ?? null) !== '',
        ));

        if ($bindings === []) {
            return true;
        }

        if (count($bindings) !== 1) {
            throw RabbitMqTopologyException::incompatibleBindings($queue, $topic);
        }

        $binding = $bindings[0];
        $arguments = $binding['arguments'] ?? [];

        if (
            ($binding['source'] ?? null) !== $topic
            || ($binding['destination_type'] ?? null) !== 'queue'
            || ($binding['routing_key'] ?? null) !== ''
            || ! is_array($arguments)
            || $arguments !== []
        ) {
            throw RabbitMqTopologyException::incompatibleBindings($queue, $topic);
        }

        return false;
    }

    /**
     * @param  list<array<string, mixed>>  $policies
     */
    private function applicableDeliveryLimit(
        string $queue,
        string $queueType,
        array $policies,
    ): ?int {
        $applicable = array_values(array_filter(
            $policies,
            fn (array $policy): bool => $this->policyApplies($policy, $queue, $queueType),
        ));

        usort(
            $applicable,
            static fn (array $left, array $right): int => ($right['priority'] ?? 0) <=> ($left['priority'] ?? 0),
        );

        if (! isset($applicable[0])) {
            return null;
        }

        // RabbitMQ chooses nondeterministically between matching policies with equal priority.
        $priority = $applicable[0]['priority'] ?? 0;
        $limits = array_map(
            $this->policyDeliveryLimit(...),
            array_filter(
                $applicable,
                static fn (array $policy): bool => ($policy['priority'] ?? 0) === $priority,
            ),
        );

        $finiteLimit = current(array_filter(
            $limits,
            static fn (?int $limit): bool => $limit !== null && $limit !== -1,
        ));

        if ($finiteLimit !== false) {
            return $finiteLimit;
        }

        return count(array_filter(
            $limits,
            static fn (?int $limit): bool => $limit === -1,
        )) === count($limits)
            ? -1
            : null;
    }

    /**
     * @param  array<string, mixed>  $policy
     */
    private function policyApplies(array $policy, string $queue, string $queueType): bool
    {
        $applyTo = $policy['apply-to'] ?? null;

        if (
            ! in_array($applyTo, ['all', 'queues', "{$queueType}_queues"], true)
            || ! is_string($policy['pattern'] ?? null)
        ) {
            return false;
        }

        $pattern = $policy['pattern'];
        $result = @preg_match('~'.str_replace('~', '\~', $pattern).'~', $queue);

        if ($result === false) {
            throw RabbitMqTopologyException::invalidPolicy(
                is_string($policy['name'] ?? null) ? $policy['name'] : 'unknown',
            );
        }

        return $result === 1;
    }

    /**
     * @param  array<string, mixed>  $policy
     */
    private function policyDeliveryLimit(array $policy): ?int
    {
        $definition = $policy['definition'] ?? null;

        return is_array($definition)
            ? $this->integerValue($definition['delivery-limit'] ?? null)
            : null;
    }

    private function integerValue(mixed $value): ?int
    {
        return is_int($value) ? $value : null;
    }
}
