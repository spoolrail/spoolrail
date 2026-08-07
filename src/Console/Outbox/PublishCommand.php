<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console\Outbox;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\Outbox\PublishOutbox;

class PublishCommand extends Command
{
    protected $signature = 'spoolrail:outbox:publish';

    protected $description = 'Publish committed Spoolrail outbox messages';

    public function handle(PublishOutbox $publishOutbox): int
    {
        $this->trap(
            fn (): array => [SIGINT, SIGTERM, SIGQUIT],
            fn () => $publishOutbox->stop(),
        );

        return $publishOutbox() ? self::SUCCESS : self::FAILURE;
    }
}
