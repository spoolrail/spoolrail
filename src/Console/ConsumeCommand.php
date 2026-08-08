<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionProcess;
use Throwable;

class ConsumeCommand extends Command
{
    protected $signature = 'spoolrail:consume {subscription}';

    protected $description = 'Consume one Spoolrail subscription process';

    protected $hidden = true;

    public function handle(
        SubscriptionConsumer $consumer,
        ExceptionHandler $exceptions,
    ): int {
        $subscription = $this->argument('subscription');

        try {
            $consumer->consume($subscription);

            $failure = ConsumerException::subscriptionStoppedUnexpectedly(
                $subscription,
            );
        } catch (Throwable $exception) {
            $failure = ConsumerException::subscriptionFailed($subscription, $exception);
        }

        try {
            $exceptions->report($failure);
        } catch (Throwable) {
            // Reporting must not prevent the supervisor from restarting this worker.
        }

        return SubscriptionProcess::REPORTED_FAILURE_EXIT_CODE;
    }
}
