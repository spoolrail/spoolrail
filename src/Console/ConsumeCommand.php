<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Illuminate\Contracts\Debug\ExceptionHandler;
use LogicException;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Subscriptions\ConsumerProcess;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Throwable;

class ConsumeCommand extends Command
{
    protected $signature = 'spoolrail:consume {subscriptions*}';

    protected $description = 'Consume Spoolrail subscriptions in one process';

    protected $hidden = true;

    public function handle(
        SubscriptionConsumer $consumer,
        ExceptionHandler $exceptions,
    ): int {
        /** @var list<string> $subscriptionNames */
        $subscriptionNames = $this->argument('subscriptions');

        if ($subscriptionNames === []) {
            throw new LogicException('A consumer process requires at least one subscription.');
        }

        $this->trap(
            fn (): array => [SIGINT, SIGTERM, SIGQUIT],
            function () use ($consumer): void {
                $consumer->stop();
            },
        );

        try {
            $consumer->consume($subscriptionNames);

            return self::SUCCESS;
        } catch (Throwable $exception) {
            $consumerException = ConsumerException::consumerProcessFailed($subscriptionNames, $exception);
        }

        try {
            $exceptions->report($consumerException);
        } catch (Throwable) {
            // Reporting must not prevent the supervisor from restarting this worker.
        }

        return ConsumerProcess::REPORTED_FAILURE_EXIT_CODE;
    }
}
