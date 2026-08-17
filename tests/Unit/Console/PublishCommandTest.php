<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Console\PublishCommand;
use Spoolrail\Spoolrail\Outbox\OutboxDispatcher;

test('requests a cooperative stop for termination signals', function (): void {
    $dispatcher = Mockery::mock(OutboxDispatcher::class);
    $dispatcher->expects('stop')->once()->with(SIGTERM);
    $dispatcher->expects('__invoke')
        ->once()
        ->with(Mockery::type(Closure::class))
        ->andReturnTrue();

    $command = Mockery::mock(PublishCommand::class)->makePartial();
    $command->expects('trap')
        ->once()
        ->withArgs(function (Closure $signals, callable $handler): bool {
            expect($signals())->toBe([SIGINT, SIGTERM, SIGQUIT]);

            $handler(SIGTERM);

            return true;
        });

    expect($command->handle($dispatcher))->toBe(PublishCommand::SUCCESS);
});
