<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Illuminate\Queue\Attributes\MaxExceptions;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Message;

#[MaxExceptions(4)]
class HandlerQueuePolicyAttributedParent implements MessageHandler
{
    public function handle(Message $message): void {}
}
