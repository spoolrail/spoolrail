<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\NoopMessageHandler;
use Spoolrail\Spoolrail\Tests\Fixtures\QueuePolicyRecordingMiddleware;
use Spoolrail\Spoolrail\Tests\Fixtures\QueuePolicyReleasingMiddleware;

test('captures handler policy rather than the handler in the Queue payload', function (): void {
    // --- Arrange ---
    configureHandlerPolicyDatabaseQueue();

    $handler = new class implements MessageHandler
    {
        public static int $constructions = 0;

        public int $maxExceptions = 3;

        public int $timeout = 120;

        public bool $failOnTimeout = true;

        public function __construct()
        {
            self::$constructions++;
        }

        public function handle(Message $message): void {}

        public function tries(): int
        {
            return 5;
        }

        /** @return list<int> */
        public function backoff(): array
        {
            return [10, 30, 60];
        }

        public function retryUntil(): DateTimeInterface
        {
            return CarbonImmutable::parse('2030-01-02 03:04:05 UTC');
        }
    };
    $handler::$constructions = 0;

    Spoolrail::subscribe('orders', 'configured-orders', $handler::class)
        ->onQueueConnection('policy-database');
    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume configured-orders')->run();

    $payload = handlerPolicyPayload();

    // --- Assert ---
    expect($payload['maxTries'])->toBe(5);
    expect($payload['backoff'])->toBe('10,30,60');
    expect($payload['maxExceptions'])->toBe(3);
    expect($payload['timeout'])->toBe(120);
    expect($payload['failOnTimeout'])->toBeTrue();
    expect($payload['retryUntil'])->toBe(CarbonImmutable::parse('2030-01-02 03:04:05 UTC')->getTimestamp());
    expect($handler::$constructions)->toBe(0);
    expect($payload['data']['command'])->not->toContain($handler::class);
});

test('preserves Laravel worker defaults when the handler declares no execution policy', function (): void {
    // --- Arrange ---
    configureHandlerPolicyDatabaseQueue();

    Spoolrail::subscribe('orders', 'default-policy-orders', NoopMessageHandler::class)
        ->onQueueConnection('policy-database');
    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume default-policy-orders')->run();

    $payload = handlerPolicyPayload();

    // --- Assert ---
    expect($payload['maxTries'])->toBeNull();
    expect($payload['backoff'])->toBeNull();
    expect($payload['maxExceptions'])->toBeNull();
    expect($payload['timeout'])->toBeNull();
    expect($payload['failOnTimeout'])->toBeFalse();
    expect($payload['retryUntil'])->toBeNull();
});

test('runs message-specific middleware in order around a handler constructed only for execution', function (): void {
    // --- Arrange ---
    config()->set('queue.default', 'sync');

    $handler = new class implements MessageHandler
    {
        public static int $constructions = 0;

        public static ?string $middlewareMessageId = null;

        public function __construct()
        {
            self::$constructions++;
        }

        public function handle(Message $message): void
        {
            QueuePolicyRecordingMiddleware::$events[] = 'handle';
        }

        /** @return list<object> */
        public function middleware(Message $message): array
        {
            self::$middlewareMessageId = $message->id;

            return [
                new QueuePolicyRecordingMiddleware('first'),
                new QueuePolicyRecordingMiddleware('second'),
            ];
        }
    };
    $handler::$constructions = 0;
    QueuePolicyRecordingMiddleware::$events = [];

    Spoolrail::subscribe('orders', 'middleware-orders', $handler::class);
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume middleware-orders')->run();

    // --- Assert ---
    expect($handler::$middlewareMessageId)->toBe($published->id);
    expect($handler::$constructions)->toBe(1);
    expect(QueuePolicyRecordingMiddleware::$events)->toBe([
        'before:first',
        'before:second',
        'handle',
        'after:second',
        'after:first',
    ]);
});

test('gives captured middleware the underlying Laravel Queue job', function (): void {
    // --- Arrange ---
    configureHandlerPolicyDatabaseQueue();

    $handler = new class implements MessageHandler
    {
        public bool $handled = false;

        public function handle(Message $message): void
        {
            $this->handled = true;
        }

        /** @return list<QueuePolicyReleasingMiddleware> */
        public function middleware(Message $message): array
        {
            return [new QueuePolicyReleasingMiddleware];
        }
    };
    app()->instance($handler::class, $handler);

    Spoolrail::subscribe('orders', 'release-orders', $handler::class)
        ->onQueueConnection('policy-database');
    Spoolrail::publish('orders', Message::make('order.created', []));
    $this->artisan('spoolrail:consume release-orders')->run();

    // --- Act ---
    $this->artisan('queue:work policy-database --once --tries=1')->run();

    $queued = DB::connection('testing')->table('policy_jobs')->first();

    // --- Assert ---
    expect($handler->handled)->toBeFalse();
    expect($queued)->not->toBeNull();
    expect($queued->attempts)->toBe(1);
    expect($queued->reserved_at)->toBeNull();
    expect($queued->available_at)->toBeGreaterThan($queued->created_at);
});

test('keeps policy extraction failure inside the source handoff boundary', function (): void {
    // --- Arrange ---
    configureHandlerPolicyDatabaseQueue();

    $handler = new class implements MessageHandler
    {
        public static bool $shouldFail = true;

        public function handle(Message $message): void {}

        public function tries(): int
        {
            if (self::$shouldFail) {
                throw new RuntimeException('Handler queue policy failed.');
            }

            return 3;
        }
    };
    $handler::$shouldFail = true;

    Spoolrail::subscribe('orders', 'failing-policy-orders', $handler::class)
        ->onQueueConnection('policy-database');
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume failing-policy-orders')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $queuedAfterFailure = DB::connection('testing')->table('policy_jobs')->count();

    $handler::$shouldFail = false;
    $this->artisan('spoolrail:consume failing-policy-orders')->run();

    $payload = handlerPolicyPayload();
    $job = unserialize($payload['data']['command']);

    // --- Assert ---
    expect($failure)->toBeInstanceOf(RuntimeException::class);
    expect($failure?->getMessage())->toBe('Handler queue policy failed.');
    expect($queuedAfterFailure)->toBe(0);
    expect($job->message->id)->toBe($published->id);
});

test('uses captured policy while resolving a replacement handler at execution', function (): void {
    // --- Arrange ---
    configureHandlerPolicyDatabaseQueue();

    $original = new class implements MessageHandler
    {
        public function handle(Message $message): void {}

        public function tries(): int
        {
            return 5;
        }
    };

    Spoolrail::subscribe('orders', 'warehouse-orders', $original::class)
        ->onQueueConnection('policy-database');
    Spoolrail::publish('orders', Message::make('order.created', []));
    $this->artisan('spoolrail:consume warehouse-orders')->run();

    $replacement = new class implements MessageHandler
    {
        public bool $handled = false;

        public function handle(Message $message): void
        {
            $this->handled = true;
        }

        public function tries(): int
        {
            throw new RuntimeException('Replacement policy must not be resolved at execution.');
        }
    };
    app()->instance($replacement::class, $replacement);

    $deployedSubscriptions = new SubscriptionRegistry;
    $deployedSubscriptions
        ->subscribe('orders', 'warehouse-orders-v2', $replacement::class)
        ->drainMessagesQueuedFor('warehouse-orders');
    app()->instance(SubscriptionRegistry::class, $deployedSubscriptions);

    $payloadAfterDeployment = handlerPolicyPayload();

    // --- Act ---
    $this->artisan('queue:work policy-database --once')->run();

    // --- Assert ---
    expect($payloadAfterDeployment['maxTries'])->toBe(5);
    expect($replacement->handled)->toBeTrue();
});

function configureHandlerPolicyDatabaseQueue(): void
{
    Schema::connection('testing')->create('policy_jobs', function (Blueprint $blueprint): void {
        $blueprint->id();
        $blueprint->string('queue')->index();
        $blueprint->longText('payload');
        $blueprint->unsignedTinyInteger('attempts');
        $blueprint->unsignedInteger('reserved_at')->nullable();
        $blueprint->unsignedInteger('available_at');
        $blueprint->unsignedInteger('created_at');
    });

    config()->set('queue.default', 'policy-database');
    config()->set('queue.connections.policy-database', [
        'driver' => 'database',
        'connection' => 'testing',
        'table' => 'policy_jobs',
        'queue' => 'default',
        'retry_after' => 90,
        'after_commit' => false,
    ]);
    config()->set('queue.failed.driver', 'null');
}

/** @return array<string, mixed> */
function handlerPolicyPayload(): array
{
    $payload = DB::connection('testing')->table('policy_jobs')->value('payload');

    return json_decode((string) $payload, true, flags: JSON_THROW_ON_ERROR);
}
