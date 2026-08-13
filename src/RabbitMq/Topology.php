<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\RabbitMq;

use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Contracts\TopologyPlan;
use Spoolrail\Spoolrail\Exceptions\RabbitMqManagementException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Spoolrail\Spoolrail\Subscriptions\Subscription;

class Topology implements CanManageTopology
{
    public function __construct(
        private ConnectionConfig $config,
        private ManagementClient $managementClient,
    ) {}

    /**
     * @param  list<Subscription>  $subscriptions
     */
    public function planSync(array $subscriptions, string $ownershipPrefix): TopologyPlan
    {
        try {
            return $this->buildPlan($subscriptions, $ownershipPrefix);
        } catch (RabbitMqManagementException $exception) {
            if ($exception->shouldRetry()) {
                throw TopologySyncRequiresRetryException::afterFailure($exception);
            }

            throw $exception;
        }
    }

    /**
     * @param  list<Subscription>  $subscriptions
     */
    private function buildPlan(array $subscriptions, string $ownershipPrefix): TopologyPlan
    {
        $requiredBindings = array_map(
            static fn (Subscription $subscription): array => [
                'exchange' => ResourceName::topic($subscription->topic()),
                'queue' => ResourceName::queue($ownershipPrefix, $subscription->name()),
            ],
            $subscriptions,
        );

        $this->ensureVersionIsSupported();

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

            $this->ensureQueueIsCompatible(
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

        return new PendingTopology(
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
        $declaredQueueNames = array_map(
            static fn (Subscription $subscription): string => ResourceName::queue(
                $ownershipPrefix,
                $subscription->name(),
            ),
            $subscriptions,
        );

        $this->ensureVersionIsSupported();

        $ownedQueueNames = [];

        foreach ($this->managementClient->queuesOwnedBy($ownershipPrefix) as $queue) {
            $queueName = $queue['name'] ?? null;

            if (is_string($queueName) && str_starts_with($queueName, $ownershipNamespace)) {
                $ownedQueueNames[] = $queueName;
            }
        }

        $undeclaredQueueNames = array_values(array_diff($ownedQueueNames, $declaredQueueNames));

        sort($undeclaredQueueNames);

        return $undeclaredQueueNames;
    }

    public function deleteSubscription(string $physicalName): void
    {
        $this->managementClient->deleteQueue($physicalName);
    }

    public function deleteTopic(string $topic): void
    {
        ResourceName::topic($topic);
        $this->ensureVersionIsSupported();

        $exchange = $this->managementClient->exchange($topic);

        if ($exchange === null) {
            throw RabbitMqTopologyException::topicMissing($topic);
        }

        $this->ensureExchangeIsCompatible($topic, $exchange);

        if (
            $this->managementClient->bindingsFromExchange($topic) !== []
            || $this->managementClient->bindingsToExchange($topic) !== []
        ) {
            throw RabbitMqTopologyException::topicHasBindings($topic);
        }

        $this->managementClient->deleteExchangeIfUnused($topic);
    }

    private function ensureVersionIsSupported(): void
    {
        $version = $this->managementClient->overview()['rabbitmq_version'] ?? null;

        if (! is_string($version)) {
            throw RabbitMqManagementException::invalidResponse(
                $this->config->connectionName,
                'reading the broker version',
            );
        }

        if (version_compare($version, Version::MINIMUM, '<')) {
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

        $this->ensureExchangeIsCompatible($exchangeName, $exchange);

        return false;
    }

    /**
     * @param  array<string, mixed>  $exchange
     */
    private function ensureExchangeIsCompatible(string $exchangeName, array $exchange): void
    {
        $requirements = [
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ];

        $setting = $this->firstIncompatibleSetting($exchange, $requirements);

        if ($setting !== null) {
            throw RabbitMqTopologyException::incompatibleExchange(
                $exchangeName,
                "[$setting] must be ".var_export($requirements[$setting], true),
            );
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
    private function ensureQueueIsCompatible(
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

        $setting = $this->firstIncompatibleSetting($queue, $requirements);

        if ($setting !== null) {
            throw RabbitMqTopologyException::incompatibleQueue(
                $queueName,
                "[$setting] must be ".var_export($requirements[$setting], true),
            );
        }

        $type = $queue['type'] ?? null;

        if (! in_array($type, ['classic', 'quorum'], true)) {
            throw RabbitMqTopologyException::incompatibleQueue(
                $queueName,
                'queue type must be classic or quorum',
            );
        }

        if ($type === 'quorum') {
            $this->ensureDeliveryLimitIsUnlimited(
                $queueName,
                $queue,
                $policies,
                $operatorPolicies,
            );
        }
    }

    /**
     * @param  array<string, mixed>  $resource
     * @param  array<string, mixed>  $requirements
     */
    private function firstIncompatibleSetting(array $resource, array $requirements): ?string
    {
        foreach ($requirements as $setting => $required) {
            if (($resource[$setting] ?? null) !== $required) {
                return $setting;
            }
        }

        return null;
    }

    /**
     * @param  array<string, mixed>  $queue
     * @param  list<array<string, mixed>>  $policies
     * @param  list<array<string, mixed>>  $operatorPolicies
     */
    private function ensureDeliveryLimitIsUnlimited(
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

        if (! in_array($applyTo, ['all', 'queues', 'quorum_queues'], true)) {
            return false;
        }

        $pattern = $policy['pattern'] ?? null;

        if (! is_string($pattern)) {
            return false;
        }

        $match = @preg_match('~'.str_replace('~', '\~', $pattern).'~', $queueName);

        if ($match === false) {
            throw RabbitMqTopologyException::invalidPolicy(
                $this->policyName($policy),
            );
        }

        return $match === 1;
    }

    /**
     * @param  array<string, mixed>  $policy
     */
    private function policyName(array $policy): string
    {
        $name = $policy['name'] ?? null;

        return is_string($name) ? $name : 'unknown';
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
