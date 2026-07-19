<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

use Spoolrail\Spoolrail\Message;

interface MessageHandler
{
    public function handle(Message $message): void;
}
