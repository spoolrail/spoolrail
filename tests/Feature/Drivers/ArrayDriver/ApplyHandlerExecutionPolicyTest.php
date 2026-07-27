<?php

declare(strict_types=1);

use Illuminate\Queue\Middleware\WithoutOverlapping;
use Illuminate\Support\Facades\DB;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithDatabaseQueue;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithDatabaseQueue::class);

beforeEach(function (): void {
    RecordingMessageHandler::reset();
});

test('captures handler policy and message-specific Laravel middleware without constructing the handler', function (): void {
    // --- Arrange ---
    $this->createJobsTable();

    Spoolrail::subscribe('orders', 'configured-orders', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume configured-orders')->run();
    $job = readQueuedHandleMessageJob();

    // --- Assert ---
    expect($job->tries)->toBe(5);
    expect($job->middleware)->toHaveCount(1);
    expect($job->middleware[0])->toBeInstanceOf(WithoutOverlapping::class);
    expect(RecordingMessageHandler::$middlewareMessageId)->toBe($published->id);
    expect(RecordingMessageHandler::$constructions)->toBe(0);
});

test('redelivers when handler policy extraction fails during handoff', function (): void {
    // --- Arrange ---
    RecordingMessageHandler::$policyFailuresRemaining = 1;

    Spoolrail::subscribe('orders', 'failing-policy-orders', RecordingMessageHandler::class);
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume failing-policy-orders')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $this->artisan('spoolrail:consume failing-policy-orders')->run();

    // --- Assert ---
    expect($failure?->getMessage())->toBe('Handler queue policy failed.');
    expect(RecordingMessageHandler::$messages)->toEqual([$published]);
});

test('uses captured policy while resolving a replacement handler at execution', function (): void {
    // --- Arrange ---
    $this->createJobsTable();

    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    $published = Spoolrail::publish('orders', Message::make('order.created', []));
    $this->artisan('spoolrail:consume warehouse-orders')->run();

    $deployedSubscriptions = new SubscriptionRegistry;
    $deployedSubscriptions
        ->subscribe('orders', 'warehouse-orders-v2', RecordingMessageHandler::class)
        ->drainMessagesQueuedFor('warehouse-orders');
    app()->instance(SubscriptionRegistry::class, $deployedSubscriptions);
    RecordingMessageHandler::$policyFailuresRemaining = 1;

    // --- Act ---
    $jobAfterDeployment = readQueuedHandleMessageJob();
    $this->artisan('queue:work database --once')->run();

    // --- Assert ---
    expect($jobAfterDeployment->tries)->toBe(5);
    expect(RecordingMessageHandler::$messages)->toEqual([$published]);
    expect(RecordingMessageHandler::$constructions)->toBe(1);
    expect(RecordingMessageHandler::$policyFailuresRemaining)->toBe(1);
});

function readQueuedHandleMessageJob(): HandleMessageJob
{
    $payload = DB::connection('testing')->table('jobs')->value('payload');
    $decoded = json_decode((string) $payload, true, flags: JSON_THROW_ON_ERROR);

    return unserialize($decoded['data']['command']);
}
