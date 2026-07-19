<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\Consumers\SubscriptionConsumer;

class ConsumeCommand extends Command
{
    protected $signature = 'spoolrail:consume {subscription}';

    protected $description = 'Consume buffered messages for a Spoolrail subscription';

    public function handle(SubscriptionConsumer $consumer): int
    {
        $consumer->consume($this->argument('subscription'));

        return self::SUCCESS;
    }
}
