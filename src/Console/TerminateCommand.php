<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\Subscriptions\TerminationSignal;

class TerminateCommand extends Command
{
    protected $signature = 'spoolrail:terminate';

    protected $description = 'Terminate Spoolrail consumers so they can be restarted';

    public function handle(TerminationSignal $signal): int
    {
        $signal->broadcast();

        $this->components->info('Broadcasting Spoolrail consumer termination signal.');

        return self::SUCCESS;
    }
}
