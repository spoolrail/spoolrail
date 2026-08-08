<?php

declare(strict_types=1);

use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\FileStore;
use Illuminate\Cache\NullStore;
use Illuminate\Cache\Repository as CacheRepository;
use Illuminate\Config\Repository as ConfigRepository;
use Illuminate\Filesystem\Filesystem;
use Spoolrail\Spoolrail\Exceptions\ConsumerException;
use Spoolrail\Spoolrail\Subscriptions\TerminationSignal;
use Symfony\Component\Process\Process;

test('shares a termination generation only with consumer processes on the same host', function (): void {
    // --- Arrange ---
    $directory = sys_get_temp_dir().'/spoolrail-termination-'.bin2hex(random_bytes(8));
    $files = new Filesystem;
    $signal = new TerminationSignal(
        new CacheRepository(new FileStore($files, $directory)),
        new ConfigRepository(['cache' => ['default' => 'file']]),
        'consumer-01',
    );
    $sameHost = new Process([
        PHP_BINARY,
        dirname(__DIR__, 2).'/Fixtures/processes/read-termination-signal.php',
        $directory,
        'consumer-01',
    ]);
    $otherHost = new Process([
        PHP_BINARY,
        dirname(__DIR__, 2).'/Fixtures/processes/read-termination-signal.php',
        $directory,
        'consumer-02',
    ]);

    try {
        // --- Act ---
        $signal->broadcast();
        $sameHost->mustRun();
        $otherHost->mustRun();
        $generation = $signal->current();

        // --- Assert ---
        expect($generation)->toBeString();
        expect($sameHost->getOutput())->toBe($generation);
        expect($otherHost->getOutput())->toBe('');
    } finally {
        $files->deleteDirectory($directory);
    }
});

test('rejects a cache store that cannot share the termination generation', function (ArrayStore|NullStore $store, string $name): void {
    $signal = new TerminationSignal(
        new CacheRepository($store),
        new ConfigRepository(['cache' => ['default' => $name]]),
        'consumer-01',
    );

    expect(fn (): ?string => $signal->current())
        ->toThrow(
            ConsumerException::class,
            "Spoolrail consumer termination requires a cross-process default cache store; [$name] is not supported.",
        );
})->with([
    'array store' => fn (): array => [new ArrayStore, 'array'],
    'null store' => fn (): array => [new NullStore, 'null'],
]);
