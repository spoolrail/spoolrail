<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Closure;
use GuzzleHttp\Handler\CurlMultiHandler;
use GuzzleHttp\Promise\PromiseInterface;
use Throwable;
use UnexpectedValueException;

/** @internal */
class GuzzleConsumerReactor
{
    /** @var array<int, PromiseInterface> */
    private array $pending = [];

    public function __construct(
        private CurlMultiHandler $handler,
        private int $idleWaitMilliseconds,
    ) {}

    /**
     * @template TResult
     *
     * @param  Closure(TResult): void  $completed
     * @param  Closure(Throwable): void  $fail
     */
    public function track(
        PromiseInterface $operation,
        Closure $completed,
        Closure $fail,
    ): void {
        $id = spl_object_id($operation);
        $this->pending[$id] = $operation;

        $operation->then(
            function (mixed $result) use ($id, $completed): void {
                unset($this->pending[$id]);

                /** @var TResult $result */
                $completed($result);
            },
            function (mixed $reason) use ($id, $fail): void {
                unset($this->pending[$id]);
                $fail($reason instanceof Throwable
                    ? $reason
                    : new UnexpectedValueException('The consumer operation failed without an exception.'));
            },
        );
    }

    public function wait(?int $wakeAfterMicroseconds = null): void
    {
        if ($this->pending !== []) {
            $this->handler->tick();

            return;
        }

        $idleWaitMicroseconds = $this->idleWaitMilliseconds * 1_000;

        usleep($wakeAfterMicroseconds === null
            ? $idleWaitMicroseconds
            : min($idleWaitMicroseconds, max(0, $wakeAfterMicroseconds)));
    }

    public function cancelPending(): void
    {
        foreach ($this->pending as $operation) {
            $operation->cancel();
        }

        $this->pending = [];
    }
}
