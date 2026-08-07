<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Illuminate\Queue\Attributes\MaxExceptions;
use Illuminate\Queue\Middleware\WithoutOverlapping;
use RuntimeException;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Message;
use Throwable;

#[MaxExceptions(4)]
class RecordingMessageHandler implements MessageHandler
{
    use HasQueueTimeout;

    /** @var list<Message> */
    public static array $messages = [];

    /** @var list<Message> */
    public static array $attemptedMessages = [];

    /** @var list<Message> */
    public static array $failedMessages = [];

    /** @var list<Throwable|null> */
    public static array $failureCauses = [];

    /** @var list<bool> */
    public static array $failedAfterHandling = [];

    public static int $attempts = 0;

    public static int $constructions = 0;

    public static ?Throwable $callbackFailure = null;

    public static bool $failWithoutException = false;

    public static int $handlerFailuresRemaining = 0;

    public static ?string $middlewareMessageId = null;

    public static int $queuePolicyFailuresRemaining = 0;

    public int $timeout = 30;

    public int $maxExceptions = 4;

    private bool $handled = false;

    public function __construct()
    {
        self::$constructions++;
    }

    public function handle(Message $message): void
    {
        self::$attempts++;
        self::$attemptedMessages[] = $message;

        if (self::$handlerFailuresRemaining > 0) {
            self::$handlerFailuresRemaining--;

            throw new RuntimeException('Handler failed.');
        }

        self::$messages[] = $message;
        $this->handled = true;
    }

    public function tries(): int
    {
        if (self::$queuePolicyFailuresRemaining > 0) {
            self::$queuePolicyFailuresRemaining--;

            throw new RuntimeException('Handler queue policy failed.');
        }

        return 5;
    }

    /** @return list<object> */
    public function middleware(Message $message): array
    {
        self::$middlewareMessageId = $message->id;

        if (self::$failWithoutException) {
            return [new FailJob];
        }

        return [new WithoutOverlapping($message->id)];
    }

    public function failed(Message $message, ?Throwable $exception): void
    {
        self::$failedMessages[] = $message;
        self::$failureCauses[] = $exception;
        self::$failedAfterHandling[] = $this->handled;

        if (self::$callbackFailure instanceof Throwable) {
            throw self::$callbackFailure;
        }
    }

    public static function reset(): void
    {
        self::$messages = [];
        self::$attemptedMessages = [];
        self::$failedMessages = [];
        self::$failureCauses = [];
        self::$failedAfterHandling = [];
        self::$attempts = 0;
        self::$constructions = 0;
        self::$callbackFailure = null;
        self::$failWithoutException = false;
        self::$handlerFailuresRemaining = 0;
        self::$middlewareMessageId = null;
        self::$queuePolicyFailuresRemaining = 0;
    }
}
