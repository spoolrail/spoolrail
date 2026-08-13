<?php

declare(strict_types=1);

use Illuminate\Http\Client\Factory;
use Spoolrail\Spoolrail\Exceptions\RabbitMqManagementException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Spoolrail\Spoolrail\RabbitMq\ConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\ManagementClient;
use Spoolrail\Spoolrail\RabbitMq\PendingTopology;

test('requests a topology retry when a RabbitMQ apply result is ambiguous', function (): void {
    // --- Arrange ---
    $http = new Factory;
    $http->fake([
        '*' => $http->response(status: 503),
    ]);
    $plan = new PendingTopology(
        new ManagementClient(new ConnectionConfig('events', []), $http),
        ['orders'],
        [],
        [],
    );

    // --- Act ---
    $caught = null;

    try {
        $plan->apply();
    } catch (TopologySyncRequiresRetryException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->getPrevious())->toBeInstanceOf(RabbitMqManagementException::class);
    expect($caught?->getPrevious()?->getMessage())
        ->toContain('HTTP 503 while creating exchange [orders]');
    expect($http->recorded())->toHaveCount(1);
});

test('fails a RabbitMQ topology apply immediately after a permanent refusal', function (): void {
    // --- Arrange ---
    $http = new Factory;
    $http->fake([
        '*' => $http->response(status: 403),
    ]);
    $plan = new PendingTopology(
        new ManagementClient(new ConnectionConfig('events', []), $http),
        ['orders'],
        [],
        [],
    );

    // --- Act ---
    $caught = null;

    try {
        $plan->apply();
    } catch (RabbitMqManagementException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->status)->toBe(403);
    expect($caught?->getMessage())->toContain('creating exchange [orders]');
    expect($http->recorded())->toHaveCount(1);
});
