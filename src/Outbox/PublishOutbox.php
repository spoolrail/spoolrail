<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Outbox;

use Illuminate\Cache\RateLimiter;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use Spoolrail\Spoolrail\Connection;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\OutboxPublicationException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\SpoolrailManager;
use Throwable;

class PublishOutbox
{
    private bool $shouldStop = false;

    /**
     * @var array<string, Connection>
     */
    private array $connections = [];

    public function __construct(
        private SpoolrailManager $spoolrail,
        private MessageEnvelope $envelope,
        private ExceptionHandler $exceptions,
        private RateLimiter $limiter,
        private Repository $config,
    ) {}

    public function __invoke(): bool
    {
        $highestId = OutboxPublication::highestId();

        if ($highestId === null) {
            return true;
        }

        return $this->publishThrough($highestId);
    }

    public function stop(): void
    {
        $this->shouldStop = true;
    }

    private function publishThrough(int $highestId): bool
    {
        [$fresh, $retries] = $this->partition(OutboxPublication::headsThrough($highestId));
        $failed = false;

        while (($head = $this->nextHead($fresh, $retries)) instanceof OutboxPublication) {
            if ($this->shouldStop) {
                break;
            }

            $publication = OutboxPublication::query()->findOrFail($head->id);

            if (! $this->publish($publication)) {
                $failed = true;

                continue;
            }

            $next = OutboxPublication::nextHeadInLane(
                $publication->connection,
                $publication->topic,
                $highestId,
            );

            if ($next instanceof OutboxPublication) {
                $this->enqueue($next, $fresh, $retries);
            }
        }

        return ! $failed;
    }

    /**
     * @param  list<OutboxPublication>  $fresh
     * @param  list<OutboxPublication>  $retries
     */
    private function nextHead(array &$fresh, array &$retries): ?OutboxPublication
    {
        return array_shift($fresh) ?? array_shift($retries);
    }

    /**
     * @param  list<OutboxPublication>  $heads
     * @return array{list<OutboxPublication>, list<OutboxPublication>}
     */
    private function partition(array $heads): array
    {
        usort(
            $heads,
            static fn (OutboxPublication $left, OutboxPublication $right): int => $left->id <=> $right->id,
        );

        $fresh = [];
        $retries = [];

        foreach ($heads as $head) {
            $this->enqueue($head, $fresh, $retries);
        }

        return [$fresh, $retries];
    }

    /**
     * @param  list<OutboxPublication>  $fresh
     * @param  list<OutboxPublication>  $retries
     */
    private function enqueue(OutboxPublication $head, array &$fresh, array &$retries): void
    {
        if ($head->last_error === null) {
            $fresh[] = $head;

            return;
        }

        $retries[] = $head;
    }

    private function publish(OutboxPublication $publication): bool
    {
        try {
            $this->connection($publication->connection)->publishStored(
                $publication->topic,
                json_encode($publication->message, JSON_THROW_ON_ERROR),
                $publication->headers,
                $publication->ordering_key,
            );
        } catch (Throwable $exception) {
            $failure = $exception instanceof PublicationException
                ? $exception
                : PublicationException::outcomeUnknown($exception);

            $publication->recordFailure($this->failureSummary($exception));
            $this->report($publication, $failure);

            return false;
        }

        $publication->delete();

        if ($publication->last_error !== null) {
            $this->logRecovery($publication);
        }

        return true;
    }

    private function connection(string $name): Connection
    {
        if (! isset($this->connections[$name])) {
            $this->spoolrail->forgetConnection($name);
            $this->connections[$name] = $this->spoolrail->connection($name);
        }

        return $this->connections[$name];
    }

    private function failureSummary(Throwable $exception): string
    {
        $message = trim((string) preg_replace('/\s+/u', ' ', mb_scrub($exception->getMessage())));

        return Str::limit($message !== '' ? $message : $exception::class, 500, '');
    }

    private function report(
        OutboxPublication $publication,
        PublicationException $failure,
    ): void {
        $exception = new OutboxPublicationException(
            outboxId: $publication->id,
            logicalMessage: $this->envelope->fromArray($publication->message),
            connectionName: $publication->connection,
            topic: $publication->topic,
            previous: $failure,
        );

        if (! $this->shouldReport($publication->id)) {
            return;
        }

        try {
            $this->exceptions->report($exception);
        } catch (Throwable) {
            // Reporting is secondary to preserving publication intent.
        }
    }

    private function shouldReport(int $outboxId): bool
    {
        $key = "spoolrail:outbox:$outboxId";
        $cooldown = $this->exceptionCooldown();

        try {
            if ($this->limiter->tooManyAttempts($key, 1)) {
                return false;
            }

            $this->limiter->hit($key, $cooldown);
        } catch (Throwable) {
            return true;
        }

        return true;
    }

    private function exceptionCooldown(): int
    {
        $cooldown = $this->config->get('spoolrail.outbox.exception_cooldown', 300);

        if (! is_int($cooldown) || $cooldown < 1) {
            throw InvalidConfigException::invalidOutboxExceptionCooldown();
        }

        return $cooldown;
    }

    private function logRecovery(OutboxPublication $publication): void
    {
        try {
            $message = $this->envelope->fromArray($publication->message);

            Log::notice('Spoolrail outbox publication recovered.', [
                'outbox_id' => $publication->id,
                'message_id' => $message->id,
                'connection' => $publication->connection,
                'topic' => $publication->topic,
            ]);
        } catch (Throwable) {
            // Recovery logging cannot restore a successfully published row.
        }
    }
}
