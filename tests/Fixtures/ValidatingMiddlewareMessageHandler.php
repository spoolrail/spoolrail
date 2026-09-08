<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Illuminate\Queue\Middleware\ThrottlesExceptions;
use InvalidArgumentException;
use Override;
use Spoolrail\Spoolrail\Message;

class ValidatingMiddlewareMessageHandler extends RecordingMessageHandler
{
    public static int $middlewareAttempts = 0;

    #[Override]
    public function tries(): int
    {
        parent::tries();

        return 2;
    }

    /** @return list<object> */
    #[Override]
    public function middleware(Message $message): array
    {
        self::$middlewareAttempts++;

        if (! is_string($message->payload['account'] ?? null)) {
            throw new InvalidArgumentException('An account is required for middleware.');
        }

        return [(new ThrottlesExceptions)->by($message->payload['account'])->report()];
    }
}
