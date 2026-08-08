<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionProcess;
use Symfony\Component\Process\Process;

test('forwards child output without retaining it in the long-lived process buffer', function (): void {
    // --- Arrange ---
    $process = new Process([
        PHP_BINARY,
        '-r',
        'fwrite(STDOUT, "out"); fwrite(STDERR, "err");',
    ]);
    $subscription = new SubscriptionProcess('warehouse-orders', $process);
    $output = '';

    // --- Act ---
    $subscription->start(function (string $name, string $chunk) use (&$output): void {
        $output .= "[$name]$chunk";
    });

    while ($subscription->isRunning()) {
        usleep(1_000);
    }

    // --- Assert ---
    expect($output)->toContain('[warehouse-orders]out');
    expect($output)->toContain('[warehouse-orders]err');
    expect($process->getOutput())->toBe('');
    expect($process->getErrorOutput())->toBe('');
});

test('distinguishes a reported worker failure from an abnormal process exit', function (): void {
    // --- Arrange ---
    $reportedProcess = new SubscriptionProcess(
        'warehouse-orders',
        new Process([
            PHP_BINARY,
            '-r',
            'exit('.SubscriptionProcess::REPORTED_FAILURE_EXIT_CODE.');',
        ]),
    );
    $abnormalProcess = new SubscriptionProcess(
        'billing-orders',
        new Process([PHP_BINARY, '-r', 'exit(1);']),
    );

    // --- Act ---
    $reportedProcess->start(static function (): void {});
    $abnormalProcess->start(static function (): void {});

    while ($reportedProcess->isRunning() || $abnormalProcess->isRunning()) {
        usleep(1_000);
    }

    // --- Assert ---
    expect($reportedProcess->unreportedFailure())->toBeNull();
    expect($abnormalProcess->unreportedFailure())
        ->toBeInstanceOf(ConsumerException::class)
        ->getMessage()->toBe('Spoolrail subscription [billing-orders] process exited with code [1].');
});
