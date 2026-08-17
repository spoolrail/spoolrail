<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use RuntimeException;
use Spoolrail\Spoolrail\Outbox\OutboxAssignment;
use Spoolrail\Spoolrail\Outbox\PublishOutbox;

class PublishLanesCommand extends Command
{
    protected $signature = 'spoolrail:publish-lanes';

    protected $description = 'Publish assigned Spoolrail outbox lanes';

    protected $hidden = true;

    public function handle(PublishOutbox $publisher): int
    {
        $assignment = $this->readAssignment();

        $this->trap(
            fn (): array => [SIGINT, SIGTERM, SIGQUIT],
            fn () => $publisher->stop(),
        );

        return $publisher($assignment) ? self::SUCCESS : self::FAILURE;
    }

    protected function readAssignment(): OutboxAssignment
    {
        $input = file_get_contents('php://stdin');

        if ($input === false) {
            throw new RuntimeException('Spoolrail could not read the outbox worker assignment.');
        }

        return OutboxAssignment::fromJson($input);
    }
}
