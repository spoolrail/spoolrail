<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Contracts;

use Closure;

interface Driver
{
    public function publish(string $topic, string $body): void;

    /**
     * The driver retains its native receipt while invoking the handoff with the serialized body.
     * A normal handoff return authorizes the driver to settle the source delivery.
     * A handoff exception must leave it unsettled, stop consumption, and propagate unchanged.
     * A settlement failure must also stop consumption and propagate.
     *
     * @param  Closure(string): void  $handoff
     */
    public function consume(string $subscription, Closure $handoff): void;
}
