<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Spoolrail\Spoolrail\Contracts\ManagedTopology;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\RabbitMqManagementException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\Subscriptions\Subscription;

readonly class RabbitMqTopology implements ManagedTopology
{
    public function __construct(
        private RabbitMqConnectionConfig $config,
        private RabbitMqManagementClient $managementClient,
    ) {}

    /**
     * @param  list<Subscription>  $subscriptions
     */
    public function planSync(array $subscriptions, string $ownershipPrefix): TopologyPlan
    {
        $requiredBindings = array_map(
            static fn (Subscription $subscription): array => [
                'exchange' => RabbitMqName::topic($subscription->topic()),
                'queue' => RabbitMqName::queue($ownershipPrefix, $subscription->name()),
            ],
            $subscriptions,
        );

        $this->assertSupportedVersion();

        $defaultQueueType = null;
        $policies = $this->managementClient->policies();
        $operatorPolicies = $this->managementClient->operatorPolicies();
        $missingExchanges = [];
        $missingQueues = [];
        $missingBindings = [];
        $inspectedExchanges = [];

        foreach ($requiredBindings as $requiredBinding) {
            $exchangeName = $requiredBinding['exchange'];
            $queueName = $requiredBinding['queue'];

            if (! isset($inspectedExchanges[$exchangeName])) {
                if ($this->exchangeNeedsCreation($exchangeName)) {
                    $missingExchanges[] = $exchangeName;
                }

                $inspectedExchanges[$exchangeName] = true;
            }

            $queue = $this->managementClient->queue($queueName);

            if ($queue === null) {
                $arguments = $this->queueDeclarationArguments(
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
                    'exchange' => $exchangeName,
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

            if ($this->bindingNeedsCreation($queueName, $exchangeName)) {
                $missingBindings[] = [
                    'exchange' => $exchangeName,
                    'queue' => $queueName,
                ];
            }
        }

        return new RabbitMqTopologyPlan(
            $this->managementClient,
            $missingExchanges,
            $missingQueues,
            $missingBindings,
        );
    }

    /**
     * @param  list<Subscription>  $subscriptions
     * @return list<string> Physical subscription resource names not represented by the declarations
     */
    public function undeclaredSubscriptionResourceNames(
        array $subscriptions,
        string $ownershipPrefix,
    ): array {
        $ownershipNamespace = "$ownershipPrefix-";
        $declaredQueueNames = [];

        foreach ($subscriptions as $subscription) {
            $declaredQueueNames[RabbitMqName::queue($ownershipPrefix, $subscription->name())] = true;
        }

        $this->assertSupportedVersion();

        $undeclaredQueueNames = [];

        foreach ($this->managementClient->queuesOwnedBy($ownershipPrefix) as $queue) {
            $queueName = $queue['name'] ?? null;

            if (
                is_string($queueName)
                && str_starts_with($queueName, $ownershipNamespace)
                && ! isset($declaredQueueNames[$queueName])
            ) {
                $undeclaredQueueNames[] = $queueName;
            }
        }

        sort($undeclaredQueueNames);

        return $undeclaredQueueNames;
    }

    public function deleteSubscription(string $physicalName): void
    {
        $this->managementClient->deleteQueue($physicalName);
    }

    public function deleteTopic(string $topic): void
    {
        RabbitMqName::topic($topic);
        $this->assertSupportedVersion();

        $exchange = $this->managementClient->exchange($topic);

        if ($exchange === null) {
            throw RabbitMqTopologyException::topicMissing($topic);
        }

        $this->assertCompatibleExchange($topic, $exchange);

        if (
            $this->managementClient->exchangeSourceBindings($topic) !== []
            || $this->managementClient->exchangeDestinationBindings($topic) !== []
        ) {
            throw RabbitMqTopologyException::topicHasBindings($topic);
        }

        $this->managementClient->deleteExchangeIfUnused($topic);
    }

    private function assertSupportedVersion(): void
    {
        $version = $this->managementClient->overview()['rabbitmq_version'] ?? null;

        if (! is_string($version)) {
            throw RabbitMqManagementException::invalidResponse(
                $this->config->connectionName,
                'reading the broker version',
            );
        }

        if (version_compare($version, RabbitMqVersion::MINIMUM, '<')) {
            throw RabbitMqTopologyException::unsupportedVersion($version);
        }
    }

    private function defaultQueueType(): string
    {
        $type = $this->managementClient->virtualHost()['default_queue_type'] ?? null;

        if (! in_array($type, ['classic', 'quorum'], true)) {
            throw RabbitMqTopologyException::unsupportedDefaultQueueType(
                is_string($type) ? $type : 'unknown',
            );
        }

        return $type;
    }

    private function exchangeNeedsCreation(string $exchangeName): bool
    {
        $exchange = $this->managementClient->exchange($exchangeName);

        if ($exchange === null) {
            return true;
        }

        $this->assertCompatibleExchange($exchangeName, $exchange);

        return false;
    }

    /**
     * @param  array<string, mixed>  $exchange
     */
    private function assertCompatibleExchange(string $exchangeName, array $exchange): void
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
                    $exchangeName,
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
    private function queueDeclarationArguments(
        string $queueName,
        string $defaultQueueType,
        array $policies,
        array $operatorPolicies,
    ): array {
        if ($defaultQueueType === 'classic') {
            return [];
        }

        $finiteLimit = $this->firstFiniteDeliveryLimit([
            $this->applicableDeliveryLimit($queueName, $operatorPolicies),
            $this->applicableDeliveryLimit($queueName, $policies),
        ]);

        if ($finiteLimit !== null) {
            throw RabbitMqTopologyException::finiteDeliveryLimit($queueName, $finiteLimit);
        }

        return ['x-delivery-limit' => -1];
    }

    /**
     * @param  array<string, mixed>  $queue
     * @param  list<array<string, mixed>>  $policies
     * @param  list<array<string, mixed>>  $operatorPolicies
     */
    private function assertCompatibleQueue(
        string $queueName,
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
                    $queueName,
                    "[$setting] must be ".var_export($required, true),
                );
            }
        }

        $type = $queue['type'] ?? null;

        if (! in_array($type, ['classic', 'quorum'], true)) {
            throw RabbitMqTopologyException::incompatibleQueue(
                $queueName,
                'queue type must be classic or quorum',
            );
        }

        if ($type === 'quorum') {
            $this->assertUnlimitedDeliveryLimit(
                $queueName,
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
        string $queueName,
        array $queue,
        array $policies,
        array $operatorPolicies,
    ): void {
        $arguments = $queue['arguments'] ?? [];
        $declaredLimit = is_array($arguments)
            ? $this->integerOrNull($arguments['x-delivery-limit'] ?? null)
            : null;

        $limits = [
            $declaredLimit,
            $this->applicableDeliveryLimit($queueName, $policies),
            $this->applicableDeliveryLimit($queueName, $operatorPolicies),
        ];

        $finiteLimit = $this->firstFiniteDeliveryLimit($limits);

        if ($finiteLimit !== null) {
            throw RabbitMqTopologyException::finiteDeliveryLimit($queueName, $finiteLimit);
        }

        if (in_array(-1, $limits, true)) {
            return;
        }

        throw RabbitMqTopologyException::indeterminateDeliveryLimit($queueName);
    }

    private function bindingNeedsCreation(string $queueName, string $exchangeName): bool
    {
        $bindings = array_values(array_filter(
            $this->managementClient->queueBindings($queueName),
            static fn (array $binding): bool => ($binding['source'] ?? null) !== '',
        ));

        if ($bindings === []) {
            return true;
        }

        if (count($bindings) !== 1) {
            throw RabbitMqTopologyException::incompatibleBindings($queueName, $exchangeName);
        }

        $binding = $bindings[0];
        $requirements = [
            'source' => $exchangeName,
            'destination_type' => 'queue',
            'routing_key' => '',
            'arguments' => [],
        ];

        if (array_intersect_key($binding, $requirements) !== $requirements) {
            throw RabbitMqTopologyException::incompatibleBindings($queueName, $exchangeName);
        }

        return false;
    }

    /**
     * @param  list<array<string, mixed>>  $policies
     */
    private function applicableDeliveryLimit(
        string $queueName,
        array $policies,
    ): ?int {
        $applicable = array_values(array_filter(
            $policies,
            fn (array $policy): bool => $this->policyApplies($policy, $queueName),
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
        $limits = array_values(array_map(
            $this->policyDeliveryLimit(...),
            array_filter(
                $applicable,
                static fn (array $policy): bool => ($policy['priority'] ?? 0) === $priority,
            ),
        ));

        $finiteLimit = $this->firstFiniteDeliveryLimit($limits);

        if ($finiteLimit !== null) {
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
     * @param  list<?int>  $limits
     */
    private function firstFiniteDeliveryLimit(array $limits): ?int
    {
        foreach ($limits as $limit) {
            if ($limit !== null && $limit !== -1) {
                return $limit;
            }
        }

        return null;
    }

    /**
     * @param  array<string, mixed>  $policy
     */
    private function policyApplies(array $policy, string $queueName): bool
    {
        $applyTo = $policy['apply-to'] ?? null;

        if (
            ! in_array($applyTo, ['all', 'queues', 'quorum_queues'], true)
            || ! is_string($policy['pattern'] ?? null)
        ) {
            return false;
        }

        $pattern = $policy['pattern'];
        $match = @preg_match('~'.str_replace('~', '\~', $pattern).'~', $queueName);

        if ($match === false) {
            throw RabbitMqTopologyException::invalidPolicy(
                is_string($policy['name'] ?? null) ? $policy['name'] : 'unknown',
            );
        }

        return $match === 1;
    }

    /**
     * @param  array<string, mixed>  $policy
     */
    private function policyDeliveryLimit(array $policy): ?int
    {
        $definition = $policy['definition'] ?? null;

        return is_array($definition)
            ? $this->integerOrNull($definition['delivery-limit'] ?? null)
            : null;
    }

    private function integerOrNull(mixed $value): ?int
    {
        return is_int($value) ? $value : null;
    }
}
