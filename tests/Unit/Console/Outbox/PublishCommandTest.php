<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Console\Outbox\PublishCommand;
use Spoolrail\Spoolrail\Outbox\PublishOutbox;

test('requests a cooperative stop for termination signals', function (): void {
    $publishOutbox = Mockery::mock(PublishOutbox::class);
    $publishOutbox->expects('stop')->once();
    $publishOutbox->expects('__invoke')->once()->andReturnTrue();

    $command = Mockery::mock(PublishCommand::class)->makePartial();
    $command->expects('trap')
        ->once()
        ->withArgs(function (Closure $signals, callable $handler): bool {
            expect($signals())->toBe([SIGINT, SIGTERM, SIGQUIT]);

            $handler(SIGTERM);

            return true;
        });

    expect($command->handle($publishOutbox))->toBe(PublishCommand::SUCCESS);
})->skip(
    ! function_exists('pcntl_signal')
        || ! defined('SIGINT')
        || ! defined('SIGTERM')
        || ! defined('SIGQUIT'),
    'PCNTL signal handling is unavailable.',
);
