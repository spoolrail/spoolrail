<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Closure;
use Override;
use Spoolrail\Spoolrail\Message;

class LongRunningMessageHandler extends RecordingMessageHandler
{
    public static ?Closure $duringHandling = null;

    public int $timeout = 120;

    #[Override]
    public function handle(Message $message): void
    {
        parent::handle($message);

        if (self::$duringHandling instanceof Closure) {
            $probe = self::$duringHandling;
            self::$duringHandling = null;

            $probe($message);
        }
    }

    /** @return list<never> */
    #[Override]
    public function middleware(Message $message): array
    {
        return [];
    }
}
