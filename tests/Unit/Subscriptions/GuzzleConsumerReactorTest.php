<?php

declare(strict_types=1);

use GuzzleHttp\Handler\CurlMultiHandler;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\RejectedPromise;
use GuzzleHttp\Promise\Utils;
use Spoolrail\Spoolrail\Subscriptions\GuzzleConsumerReactor;

test('cancels every pending consumer operation during shutdown', function (): void {
    // --- Arrange ---
    $cancelled = [];
    $failures = [];
    $reactor = new GuzzleConsumerReactor(
        new CurlMultiHandler(['select_timeout' => 0.001]),
        100,
    );

    foreach (['receive', 'settlement'] as $operation) {
        $promise = new Promise(
            cancelFn: function () use (&$cancelled, $operation): void {
                $cancelled[] = $operation;
            },
        );
        $reactor->track(
            $promise,
            static function (): void {},
            function (Throwable $exception) use (&$failures): void {
                $failures[] = $exception;
            },
        );
    }

    // --- Act ---
    $reactor->cancelPending();
    Utils::queue()->run();

    // --- Assert ---
    expect($cancelled)->toBe(['receive', 'settlement']);
    expect($failures)->toHaveCount(2);
});

test('turns a non-exception promise rejection into a throwable failure', function (): void {
    // --- Arrange ---
    $failure = null;
    $reactor = new GuzzleConsumerReactor(
        new CurlMultiHandler(['select_timeout' => 0.001]),
        100,
    );

    $reactor->track(
        new RejectedPromise('invalid rejection'),
        static function (): void {},
        function (Throwable $exception) use (&$failure): void {
            $failure = $exception;
        },
    );

    // --- Act ---
    $reactor->wait();

    // --- Assert ---
    expect($failure)->toBeInstanceOf(UnexpectedValueException::class);
    expect($failure->getMessage())->toBe('The consumer operation failed without an exception.');
});
