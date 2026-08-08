<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Subscriptions\ConsumerSupervisor;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;

class SpoolrailCommand extends Command
{
    protected $signature = 'spoolrail {subscription?} {--connection=}';

    protected $description = 'Consume Spoolrail subscriptions';

    public function handle(
        SpoolrailManager $manager,
        SubscriptionRegistry $subscriptions,
        SubscriptionConsumer $consumer,
        ConsumerSupervisor $supervisor,
    ): int {
        $subscriptionName = $this->subscriptionArgument();

        if ($subscriptionName !== null) {
            return $this->consumeSubscription(
                $subscriptionName,
                $manager,
                $subscriptions,
                $consumer,
                $supervisor,
            );
        }

        return $this->consumeConnection($manager, $subscriptions, $supervisor);
    }

    private function consumeSubscription(
        string $subscriptionName,
        SpoolrailManager $manager,
        SubscriptionRegistry $subscriptions,
        SubscriptionConsumer $consumer,
        ConsumerSupervisor $supervisor,
    ): int {
        if ($this->connectionOption() !== null) {
            throw ConsumerException::connectionOptionWithSubscription();
        }

        $subscription = $subscriptions->findOrFail($subscriptionName);
        $connectionName = $subscription->connectionName($manager->defaultConnectionName());

        if ($manager->driverName($connectionName) === 'array') {
            $consumer->consume($subscriptionName);

            return self::SUCCESS;
        }

        return $this->supervise($supervisor, [$subscriptionName]);
    }

    private function consumeConnection(
        SpoolrailManager $manager,
        SubscriptionRegistry $subscriptions,
        ConsumerSupervisor $supervisor,
    ): int {
        $defaultConnectionName = $manager->defaultConnectionName();
        $connectionName = $this->connectionOption() ?? $defaultConnectionName;

        if ($manager->driverName($connectionName) === 'array') {
            throw ConsumerException::arrayConnectionCannotBeSupervised();
        }

        $subscriptionNames = array_values(array_map(
            static fn (Subscription $subscription): string => $subscription->name(),
            array_filter(
                $subscriptions->all(),
                static fn (Subscription $subscription): bool => $subscription->connectionName(
                    $defaultConnectionName,
                ) === $connectionName,
            ),
        ));

        if ($subscriptionNames === []) {
            throw ConsumerException::connectionHasNoSubscriptions($connectionName);
        }

        return $this->supervise($supervisor, $subscriptionNames);
    }

    /**
     * @param  list<string>  $subscriptionNames
     */
    private function supervise(ConsumerSupervisor $supervisor, array $subscriptionNames): int
    {
        $this->trap(
            fn (): array => [SIGINT, SIGTERM, SIGQUIT],
            fn (int $signal) => $supervisor->stop($signal),
        );

        $writeOutput = function (string $subscription, string $output): void {
            $this->output->write("[$subscription] $output");
        };

        return $supervisor->supervise($subscriptionNames, $writeOutput)
            ? self::SUCCESS
            : self::FAILURE;
    }

    private function subscriptionArgument(): ?string
    {
        $value = $this->argument('subscription');

        return is_string($value) ? $value : null;
    }

    private function connectionOption(): ?string
    {
        $value = $this->option('connection');

        return is_string($value) ? $value : null;
    }
}
