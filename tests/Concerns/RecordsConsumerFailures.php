<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Concerns;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Mockery;
use Throwable;

trait RecordsConsumerFailures
{
    /** @var list<Throwable> */
    protected array $consumerFailures = [];

    protected function setUpRecordsConsumerFailures(): void
    {
        $exceptions = Mockery::mock(ExceptionHandler::class);
        $exceptions->shouldReceive('report')->andReturnUsing(
            function (Throwable $exception): void {
                $this->consumerFailures[] = $exception;
            },
        );

        app()->instance(ExceptionHandler::class, $exceptions);
    }
}
