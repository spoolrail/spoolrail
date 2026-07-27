<?php

declare(strict_types=1);

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\Exceptions\ManagedTopologyUnavailableException;

test('rejects blank option values', function (array $parameters, string $message): void {
    $exitCode = $this->artisan('spoolrail:delete-undeclared-subscriptions', $parameters)
        ->expectsOutputToContain($message)
        ->run();

    expect($exitCode)->toBe(Command::FAILURE);
})->with([
    'connection' => [
        ['--connection' => ' '],
        'The --connection option must name a Spoolrail connection.',
    ],
    'retired prefix' => [
        ['--retired-prefix' => ' '],
        'The --retired-prefix option must name a former ownership prefix.',
    ],
]);

test('rejects a connection without package-managed topology', function (): void {
    expect(fn () => $this->artisan('spoolrail:delete-undeclared-subscriptions', [
        '--connection' => 'array',
    ])->run())->toThrow(
        ManagedTopologyUnavailableException::class,
        'Spoolrail connection [array] does not provide package-managed topology.',
    );
});
