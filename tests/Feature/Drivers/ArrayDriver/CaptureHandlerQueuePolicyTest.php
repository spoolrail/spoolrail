<?php

declare(strict_types=1);

use Illuminate\Queue\Middleware\WithoutOverlapping;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Queue;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithDatabaseQueue;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;
use Spoolrail\Spoolrail\Tests\Fixtures\ValidatingMiddlewareMessageHandler;
use Spoolrail\Spoolrail\TransportContext;

uses(InteractsWithDatabaseQueue::class);

beforeEach(function (): void {
    RecordingMessageHandler::reset();
    ValidatingMiddlewareMessageHandler::$middlewareAttempts = 0;
});

test('captures handler Queue policy without constructing the handler or its middleware', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'configured-orders', ValidatingMiddlewareMessageHandler::class)
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', ['account' => 'warehouse']));

    // --- Act ---
    $this->artisan('spoolrail configured-orders')->run();
    $job = readQueuedHandleMessageJob();

    // --- Assert ---
    expect($job->tries)->toBe(2);
    expect($job->middleware)->toBeNull();
    expect(ValidatingMiddlewareMessageHandler::$middlewareAttempts)->toBe(0);
    expect(RecordingMessageHandler::$constructions)->toBe(0);
});

test('redelivers when handler Queue policy capture fails during handoff', function (): void {
    // --- Arrange ---
    config()->set('queue.default', 'sync');

    RecordingMessageHandler::$queuePolicyFailuresRemaining = 1;

    Spoolrail::subscribe('orders', 'failing-policy-orders', RecordingMessageHandler::class);
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail failing-policy-orders')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$messages)->toHaveCount(1);
    expect(RecordingMessageHandler::$messages[0]->id)->toBe($published->id);
});

test('uses captured Queue policy and current middleware while resolving a replacement handler at execution', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onQueueConnection('database');
    $published = Spoolrail::publish('orders', Message::make('order.created', ['account' => 'warehouse']));
    $this->artisan('spoolrail warehouse-orders')->run();

    $deployedSubscriptions = new SubscriptionRegistry;
    $deployedSubscriptions
        ->subscribe('orders', 'warehouse-orders-v2', ValidatingMiddlewareMessageHandler::class)
        ->drainMessagesQueuedFor('warehouse-orders');
    app()->instance(SubscriptionRegistry::class, $deployedSubscriptions);
    RecordingMessageHandler::$queuePolicyFailuresRemaining = 1;

    // --- Act ---
    $jobAfterDeployment = readQueuedHandleMessageJob();
    $this->artisan('queue:work database --once')->run();

    // --- Assert ---
    expect($jobAfterDeployment->tries)->toBe(5);
    expect(RecordingMessageHandler::$messages)->toHaveCount(1);
    expect(RecordingMessageHandler::$messages[0]->id)->toBe($published->id);
    expect(RecordingMessageHandler::$messages[0]->transport?->subscription)
        ->toBe('warehouse-orders');
    expect(RecordingMessageHandler::$constructions)->toBe(1);
    expect(RecordingMessageHandler::$queuePolicyFailuresRemaining)->toBe(1);
    expect(ValidatingMiddlewareMessageHandler::$middlewareAttempts)->toBe(1);
});

test('retries middleware construction failures in Laravel queue before reporting terminal failure', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'validated-orders', ValidatingMiddlewareMessageHandler::class)
        ->onQueueConnection('database');
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail validated-orders')->run();
    $this->artisan('queue:work database --once --sleep=0')->run();

    // --- Assert ---
    expect(DB::connection('testing')->table('jobs')->value('attempts'))->toBe(1);
    expect(RecordingMessageHandler::$failedMessages)->toBe([]);

    // --- Act ---
    $this->artisan('queue:work database --once --sleep=0')->run();
    $this->artisan('spoolrail validated-orders')->run();

    // --- Assert ---
    expect(ValidatingMiddlewareMessageHandler::$middlewareAttempts)->toBe(2);
    expect(RecordingMessageHandler::$messages)->toBe([]);
    expect(RecordingMessageHandler::$failedMessages)->toHaveCount(1);
    expect(RecordingMessageHandler::$failedMessages[0]->id)->toBe($published->id);
    expect(RecordingMessageHandler::$failureCauses[0])->toBeInstanceOf(InvalidArgumentException::class);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

test('applies legacy captured middleware once without constructing current middleware', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'legacy-orders', ValidatingMiddlewareMessageHandler::class);
    $message = Message::make('order.created', [])
        ->withTransport(new TransportContext(
            driver: 'array',
            connectionName: 'array',
            topic: 'orders',
            subscription: 'legacy-orders',
            headers: [],
        ));
    $job = new HandleMessageJob($message);
    $job->tries = 2;
    $middleware = new WithoutOverlapping($message->id);
    $job->middleware = [$middleware];
    Queue::connection('database')->push($job);
    $lock = Cache::lock($middleware->getLockKey($job));
    $lock->get();

    // --- Act ---
    $this->artisan('queue:work database --once --sleep=0')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$messages)->toBe([]);
    expect(DB::connection('testing')->table('jobs')->value('attempts'))->toBe(1);

    // --- Act ---
    $lock->release();
    $this->artisan('queue:work database --once --sleep=0')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$messages)->toHaveCount(1);
    expect(RecordingMessageHandler::$messages[0]->id)->toBe($message->id);
    expect(ValidatingMiddlewareMessageHandler::$middlewareAttempts)->toBe(0);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

test('preserves an empty captured middleware list in legacy queued jobs', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'legacy-orders', ValidatingMiddlewareMessageHandler::class);
    $message = Message::make('order.created', [])
        ->withTransport(new TransportContext(
            driver: 'array',
            connectionName: 'array',
            topic: 'orders',
            subscription: 'legacy-orders',
            headers: [],
        ));
    $job = new HandleMessageJob($message);
    $job->middleware = [];
    Queue::connection('database')->push($job);

    // --- Act ---
    $this->artisan('queue:work database --once --sleep=0')->run();

    // --- Assert ---
    expect(RecordingMessageHandler::$messages)->toHaveCount(1);
    expect(RecordingMessageHandler::$messages[0]->id)->toBe($message->id);
    expect(ValidatingMiddlewareMessageHandler::$middlewareAttempts)->toBe(0);
    expect(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

function readQueuedHandleMessageJob(): HandleMessageJob
{
    $payload = DB::connection('testing')->table('jobs')->value('payload');
    $payloadData = json_decode((string) $payload, true, flags: JSON_THROW_ON_ERROR);

    return unserialize($payloadData['data']['command']);
}
