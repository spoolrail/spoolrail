<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\Outbox\OutboxDispatcher;

class PublishCommand extends Command
{
    protected $signature = 'spoolrail:publish';

    protected $description = 'Publish committed Spoolrail outbox messages';

    public function handle(OutboxDispatcher $dispatcher): int
    {
        $this->trap(
            fn (): array => [SIGINT, SIGTERM, SIGQUIT],
            fn (int $signal) => $dispatcher->stop($signal),
        );

        return $dispatcher(
            fn (string $output) => $this->output->write($output),
        ) ? self::SUCCESS : self::FAILURE;
    }
}
