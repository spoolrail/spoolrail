<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Subscriptions\ConsumerProcess;
use Symfony\Component\Process\Process;

test('labels grouped child output without retaining process buffers', function (): void {
    // --- Arrange ---
    $process = new Process([
        PHP_BINARY,
        '-r',
        'fwrite(STDOUT, "out"); fwrite(STDERR, "err");',
    ]);
    $consumer = new ConsumerProcess(
        ['warehouse-orders', 'billing-orders'],
        $process,
    );
    $output = '';

    // --- Act ---
    $consumer->start(function (string $group, string $chunk) use (&$output): void {
        $output .= "[$group]$chunk";
    });

    while ($consumer->isRunning()) {
        usleep(1_000);
    }

    // --- Assert ---
    expect($output)->toContain('[warehouse-orders,billing-orders]out');
    expect($output)->toContain('[warehouse-orders,billing-orders]err');
    expect($process->getOutput())->toBe('');
    expect($process->getErrorOutput())->toBe('');
});

test('distinguishes a reported grouped failure from an abnormal process exit', function (): void {
    // --- Arrange ---
    $reported = new ConsumerProcess(
        ['warehouse-orders', 'billing-orders'],
        new Process([
            PHP_BINARY,
            '-r',
            'exit('.ConsumerProcess::REPORTED_FAILURE_EXIT_CODE.');',
        ]),
    );
    $abnormal = new ConsumerProcess(
        ['warehouse-orders', 'billing-orders'],
        new Process([PHP_BINARY, '-r', 'exit(1);']),
    );

    // --- Act ---
    $reported->start(static function (): void {});
    $abnormal->start(static function (): void {});

    while ($reported->isRunning() || $abnormal->isRunning()) {
        usleep(1_000);
    }

    // --- Assert ---
    expect($reported->unreportedFailure())->toBeNull();
    expect($abnormal->unreportedFailure())
        ->toBeInstanceOf(ConsumerException::class)
        ->getMessage()->toContain('warehouse-orders, billing-orders');
});
