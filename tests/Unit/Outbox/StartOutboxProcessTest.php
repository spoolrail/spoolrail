<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Outbox\OutboxAssignment;
use Spoolrail\Spoolrail\Outbox\StartOutboxProcess;

test('starts the lane-publishing command with its assignment on standard input', function (): void {
    // --- Arrange ---
    $assignment = new OutboxAssignment(19, [3, 11]);
    $startProcess = new StartOutboxProcess(
        app(),
        dirname(__DIR__, 2).'/Fixtures/processes/read-outbox-assignment.php',
    );
    $startProcess->ensureSupported();
    $output = '';

    // --- Act ---
    $process = $startProcess(
        $assignment,
        function (string $chunk) use (&$output): void {
            $output .= $chunk;
        },
    );
    $exitCode = $process->wait();

    // --- Assert ---
    $result = json_decode($output, true, flags: JSON_THROW_ON_ERROR);

    expect($exitCode)->toBe(0);
    expect($result['arguments'])->toBe(['spoolrail:publish-lanes']);
    expect(OutboxAssignment::fromJson($result['input']))->toEqual($assignment);
});
