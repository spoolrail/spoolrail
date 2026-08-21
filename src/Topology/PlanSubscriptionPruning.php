<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use InvalidArgumentException;
use LogicException;
use Spoolrail\Spoolrail\Exceptions\SubscriptionPruningException;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

class PlanSubscriptionPruning
{
    public function __construct(
        private SpoolrailManager $manager,
        private SubscriptionRegistry $subscriptions,
        private OwnershipPrefix $prefix,
    ) {}

    public function __invoke(string $connectionName, ?string $retiredPrefix): SubscriptionPruningPlan
    {
        $topology = $this->manager->connection($connectionName)->topology()
            ?? throw new LogicException("Spoolrail connection [$connectionName] does not provide package-managed topology.");

        $targetPrefix = $this->targetPrefix($retiredPrefix);
        $declaredSubscriptions = $retiredPrefix === null
            ? $this->declaredSubscriptions($connectionName)
            : [];

        if ($retiredPrefix === null && $declaredSubscriptions === []) {
            throw SubscriptionPruningException::noDeclaredSubscriptions($connectionName);
        }

        return new SubscriptionPruningPlan(
            $topology,
            $targetPrefix,
            $topology->undeclaredSubscriptionResourceNames(
                $declaredSubscriptions,
                $targetPrefix,
            ),
        );
    }

    private function targetPrefix(?string $retiredPrefix): string
    {
        $currentPrefix = $this->prefix->current();

        if ($retiredPrefix === null) {
            return $currentPrefix;
        }

        $targetPrefix = $this->prefix->validateFormer($retiredPrefix);

        if ($targetPrefix === $currentPrefix) {
            throw new InvalidArgumentException(
                "Ownership prefix [$currentPrefix] is current and cannot be supplied as a retired prefix.",
            );
        }

        return $targetPrefix;
    }

    /**
     * @return list<Subscription>
     */
    private function declaredSubscriptions(string $connectionName): array
    {
        $defaultConnectionName = $this->manager->defaultConnectionName();

        return array_values(array_filter(
            $this->subscriptions->all(),
            static fn (Subscription $subscription): bool => $subscription->connectionName($defaultConnectionName) === $connectionName,
        ));
    }
}
