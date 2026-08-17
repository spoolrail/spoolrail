<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Console\PublishLanesCommand;
use Spoolrail\Spoolrail\Outbox\OutboxAssignment;
use Spoolrail\Spoolrail\Outbox\PublishOutbox;

test('publishes assigned lanes through the shared engine', function (): void {
    // --- Arrange ---
    $assignment = new OutboxAssignment(12, [4]);

    $publisher = Mockery::mock(PublishOutbox::class);
    $publisher->shouldNotReceive('stop');
    $publisher->expects('__invoke')->once()->with($assignment)->andReturnTrue();

    $command = Mockery::mock(PublishLanesCommand::class)
        ->makePartial()
        ->shouldAllowMockingProtectedMethods();
    $command->expects('readAssignment')->once()->andReturn($assignment);
    $command->allows('trap');

    // --- Act ---
    $exitCode = $command->handle($publisher);

    // --- Assert ---
    expect($exitCode)->toBe(PublishLanesCommand::SUCCESS);
});

test('requests a cooperative stop for termination signals', function (): void {
    // --- Arrange ---
    $assignment = new OutboxAssignment(12, [4]);

    $publisher = Mockery::mock(PublishOutbox::class);
    $publisher->expects('stop')->once();
    $publisher->allows('__invoke')->with($assignment)->andReturnTrue();

    $command = Mockery::mock(PublishLanesCommand::class)
        ->makePartial()
        ->shouldAllowMockingProtectedMethods();
    $command->allows('readAssignment')->andReturn($assignment);
    $command->expects('trap')
        ->once()
        ->withArgs(function (Closure $signals, callable $handler): bool {
            expect($signals())->toBe([SIGINT, SIGTERM, SIGQUIT]);

            $handler(SIGTERM);

            return true;
        });

    // --- Act ---
    $exitCode = $command->handle($publisher);

    // --- Assert ---
    expect($exitCode)->toBe(PublishLanesCommand::SUCCESS);
});
