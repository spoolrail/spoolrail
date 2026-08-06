<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

use Closure;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\TransportContext;

interface Driver
{
    /**
     * @param  array<string, string>  $headers
     *
     * @throws PublicationException
     */
    public function publish(string $topic, string $body, array $headers): void;

    /**
     * The driver retains its native receipt while invoking the handoff with the serialized body.
     * A normal handoff return authorizes the driver to settle the source delivery.
     * A handoff exception must leave it unsettled, stop consumption, and propagate unchanged.
     * A settlement failure must also stop consumption and report that the handoff may be repeated.
     *
     * @param  Closure(string, TransportContext): void  $handoff
     */
    public function consume(string $subscription, Closure $handoff): void;
}
