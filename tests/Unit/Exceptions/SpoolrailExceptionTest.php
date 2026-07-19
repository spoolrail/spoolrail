<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\ConnectionNotConsumableException;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigurationException;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageEnvelopeException;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageException;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Exceptions\SpoolrailException;

test('keeps package exceptions within package and caller-relevant SPL catch boundaries', function (string $exception, string $splException): void {
    // --- Arrange ---
    $packageException = SpoolrailException::class;

    // --- Act ---
    $matchesPackageBoundary = is_a($exception, $packageException, true);
    $matchesSplBoundary = is_a($exception, $splException, true);

    // --- Assert ---
    expect($matchesPackageBoundary)->toBeTrue();
    expect($matchesSplBoundary)->toBeTrue();
})->with([
    'invalid configuration' => [InvalidConfigurationException::class, InvalidArgumentException::class],
    'invalid message' => [InvalidMessageException::class, InvalidArgumentException::class],
    'invalid message envelope' => [InvalidMessageEnvelopeException::class, UnexpectedValueException::class],
    'invalid subscription' => [InvalidSubscriptionException::class, InvalidArgumentException::class],
    'non-consumable connection' => [ConnectionNotConsumableException::class, LogicException::class],
]);
