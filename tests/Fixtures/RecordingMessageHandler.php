<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Illuminate\Queue\Attributes\MaxExceptions;
use Illuminate\Queue\Middleware\WithoutOverlapping;
use RuntimeException;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Message;

#[MaxExceptions(4)]
class RecordingMessageHandler implements MessageHandler
{
    use HandlerTimeoutPolicy;

    /** @var list<Message> */
    public static array $messages = [];

    public static int $attempts = 0;

    public static int $constructions = 0;

    public static int $handlerFailuresRemaining = 0;

    public static ?string $middlewareMessageId = null;

    public static int $queuePolicyFailuresRemaining = 0;

    public int $timeout = 30;

    public function __construct()
    {
        self::$constructions++;
    }

    public function handle(Message $message): void
    {
        self::$attempts++;

        if (self::$handlerFailuresRemaining > 0) {
            self::$handlerFailuresRemaining--;

            throw new RuntimeException('Handler failed.');
        }

        self::$messages[] = $message;
    }

    public function tries(): int
    {
        if (self::$queuePolicyFailuresRemaining > 0) {
            self::$queuePolicyFailuresRemaining--;

            throw new RuntimeException('Handler queue policy failed.');
        }

        return 5;
    }

    /** @return list<WithoutOverlapping> */
    public function middleware(Message $message): array
    {
        self::$middlewareMessageId = $message->id;

        return [new WithoutOverlapping($message->id)];
    }

    public static function reset(): void
    {
        self::$messages = [];
        self::$attempts = 0;
        self::$constructions = 0;
        self::$handlerFailuresRemaining = 0;
        self::$middlewareMessageId = null;
        self::$queuePolicyFailuresRemaining = 0;
    }
}
