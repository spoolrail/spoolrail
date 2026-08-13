<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\ConsumptionException;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageEnvelopeException;
use Spoolrail\Spoolrail\Exceptions\InvalidSubscriptionException;
use Spoolrail\Spoolrail\Exceptions\MessageTooLargeException;
use Spoolrail\Spoolrail\Exceptions\PublicationException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqManagementException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologyPreflightException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;

test('keeps package exceptions within caller-relevant SPL catch boundaries', function (string $exception, string $splException): void {
    expect(is_a($exception, $splException, true))->toBeTrue();
})->with([
    'invalid config' => [InvalidConfigException::class, InvalidArgumentException::class],
    'invalid message envelope' => [InvalidMessageEnvelopeException::class, UnexpectedValueException::class],
    'invalid subscription' => [InvalidSubscriptionException::class, InvalidArgumentException::class],
    'message too large' => [MessageTooLargeException::class, LengthException::class],
    'publication failure' => [PublicationException::class, RuntimeException::class],
    'consumption failure' => [ConsumptionException::class, RuntimeException::class],
    'RabbitMQ Management API failure' => [RabbitMqManagementException::class, RuntimeException::class],
    'RabbitMQ topology failure' => [RabbitMqTopologyException::class, RuntimeException::class],
    'topology preflight failure' => [TopologyPreflightException::class, RuntimeException::class],
    'topology retry request' => [TopologySyncRequiresRetryException::class, RuntimeException::class],
]);
