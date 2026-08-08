<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Subscriptions\TerminationSignal;

test('broadcasts a package termination generation', function (): void {
    // --- Arrange ---
    $signal = Mockery::mock(TerminationSignal::class);
    $signal->expects('broadcast')->once();
    app()->instance(TerminationSignal::class, $signal);

    // --- Act ---
    $command = $this->artisan('spoolrail:terminate');

    // --- Assert ---
    $command->expectsOutputToContain('Broadcasting Spoolrail consumer termination signal.')
        ->assertSuccessful();
});
