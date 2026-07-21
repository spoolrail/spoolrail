<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Fixtures;

use Closure;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;

class QueuePolicyRecordingMiddleware
{
    /** @var list<string> */
    public static array $events = [];

    public function __construct(private readonly string $name) {}

    public function handle(HandleMessageJob $job, Closure $next): void
    {
        self::$events[] = "before:$this->name";

        $next($job);

        self::$events[] = "after:$this->name";
    }
}
