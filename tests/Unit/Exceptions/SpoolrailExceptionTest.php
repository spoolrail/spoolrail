<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\DatabaseQueueTransactionException;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigurationException;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageEnvelopeException;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageException;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;

test('keeps package exceptions within caller-relevant SPL catch boundaries', function (string $exception, string $splException): void {
    expect(is_a($exception, $splException, true))->toBeTrue();
})->with([
    'invalid configuration' => [InvalidConfigurationException::class, InvalidArgumentException::class],
    'invalid message' => [InvalidMessageException::class, InvalidArgumentException::class],
    'invalid message envelope' => [InvalidMessageEnvelopeException::class, UnexpectedValueException::class],
    'invalid subscription' => [InvalidSubscriptionException::class, InvalidArgumentException::class],
    'transactional database Queue' => [DatabaseQueueTransactionException::class, LogicException::class],
]);
