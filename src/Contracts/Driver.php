<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

use Closure;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Throwable;

/**
 * @template TReceipt
 *
 * Consumer operations invoke exactly one outcome closure, either inline or
 * later. A failure reported through $fail leaves the driver usable; unusable
 * shared transport state throws instead.
 */
interface Driver
{
    /**
     * @param  array<string, string>  $headers
     *
     * @throws PublicationException
     */
    public function publish(
        string $topic,
        string $body,
        array $headers,
        ?string $orderingKey = null,
    ): void;

    /**
     * @param  Closure(list<Delivery<TReceipt>>): void  $received
     * @param  Closure(Throwable): void  $fail
     */
    public function receive(
        string $subscription,
        Closure $received,
        Closure $fail,
    ): void;

    /**
     * @param  Delivery<TReceipt>  $delivery
     * @param  Closure(): void  $acknowledged
     * @param  Closure(Throwable): void  $fail
     */
    public function acknowledge(
        Delivery $delivery,
        Closure $acknowledged,
        Closure $fail,
    ): void;

    /**
     * @param  Delivery<TReceipt>  $delivery
     * @param  Closure(): void  $released
     * @param  Closure(Throwable): void  $fail
     */
    public function release(
        Delivery $delivery,
        Closure $released,
        Closure $fail,
    ): void;
}
