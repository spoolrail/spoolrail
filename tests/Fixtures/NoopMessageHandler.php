<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Message;

class NoopMessageHandler implements MessageHandler
{
    public function handle(Message $message): void {}
}
