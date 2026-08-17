<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Outbox;

use Closure;
use Illuminate\Contracts\Foundation\Application;
use Spoolrail\Spoolrail\Exceptions\OutboxProcessException;
use Symfony\Component\Process\Process;

class StartOutboxProcess
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
            throw OutboxProcessException::unsupportedRuntime('the [proc_open] function is unavailable.');
        }

        if (! function_exists('pcntl_signal')) {
            throw OutboxProcessException::unsupportedRuntime('the PCNTL extension is unavailable.');
        }

        if (! is_file($this->artisan)) {
            throw OutboxProcessException::unsupportedRuntime("the Artisan entrypoint [$this->artisan] does not exist.");
        }
    }

    /**
     * @param  Closure(string): void  $writeOutput
     */
    public function __invoke(OutboxAssignment $assignment, Closure $writeOutput): Process
    {
        $process = new Process([
            PHP_BINARY,
            $this->artisan,
            'spoolrail:publish-lanes',
        ], timeout: null);
        $process->setInput($assignment->toJson());
        $process->start(
            function (string $type, string $output) use ($process, $writeOutput): void {
                $writeOutput($output);

                if ($type === Process::OUT) {
                    $process->clearOutput();
                } else {
                    $process->clearErrorOutput();
                }
            },
        );

        return $process;
    }
}
