<?php

declare(strict_types=1);

use Illuminate\Console\Command;

test('rejects a blank connection option', function (): void {
    $exitCode = $this->artisan('spoolrail:delete-topic', [
        'topic' => 'orders',
        '--connection' => ' ',
    ])
        ->expectsOutputToContain('The --connection option must name a Spoolrail connection.')
        ->run();

    expect($exitCode)->toBe(Command::FAILURE);
});

test('rejects a non-portable topic before resolving managed topology', function (): void {
    expect(fn () => $this->artisan('spoolrail:delete-topic', [
        'topic' => 'orders.created',
        '--connection' => 'array',
    ])->run())->toThrow(InvalidArgumentException::class);
});

test('rejects a connection without package-managed topology', function (): void {
    expect(fn () => $this->artisan('spoolrail:delete-topic', [
        'topic' => 'orders',
        '--connection' => 'array',
    ])->run())->toThrow(
        LogicException::class,
        'Spoolrail connection [array] does not provide package-managed topology.',
    );
});
