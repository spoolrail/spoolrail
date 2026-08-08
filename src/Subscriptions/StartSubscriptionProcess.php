<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Closure;
use Illuminate\Contracts\Foundation\Application;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Symfony\Component\Process\Process;

class StartSubscriptionProcess
{
    private string $artisan;

    public function __construct(
        Application $app,
        ?string $artisan = null,
    ) {
        $this->artisan = $artisan ?? $app->basePath('artisan');
    }

    public function ensureSupported(): void
    {
        if (! function_exists('proc_open')) {
            throw ConsumerException::unsupportedProcessRuntime('the [proc_open] function is unavailable.');
        }

        if (! function_exists('pcntl_signal')) {
            throw ConsumerException::unsupportedProcessRuntime('the PCNTL extension is unavailable.');
        }

        if (! is_file($this->artisan)) {
            throw ConsumerException::unsupportedProcessRuntime("the Artisan entrypoint [$this->artisan] does not exist.");
        }
    }

    /**
     * @param  Closure(string, string): void  $writeOutput
     */
    public function __invoke(string $subscription, Closure $writeOutput): SubscriptionProcess
    {
        $process = new SubscriptionProcess(
            $subscription,
            new Process([
                PHP_BINARY,
                $this->artisan,
                'spoolrail:consume',
                $subscription,
            ], timeout: null),
        );
        $process->start($writeOutput);

        return $process;
    }
}
