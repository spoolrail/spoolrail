<?php

declare(strict_types=1);

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionRegistry;
use Spoolrail\Spoolrail\Tests\Fixtures\QueuePolicyRecordingMiddleware;
use Spoolrail\Spoolrail\Tests\Fixtures\QueuePolicyReleasingMiddleware;

test('captures handler policy without constructing the handler', function (): void {
    // --- Arrange ---
    createHandlerPolicyJobsTable();

    $handler = new class implements MessageHandler
    {
        public static int $constructions = 0;

        public function __construct()
        {
            self::$constructions++;
        }

        public function handle(Message $message): void {}

        public function tries(): int
        {
            return 5;
        }
    };
    $handler::$constructions = 0;

    Spoolrail::subscribe('orders', 'configured-orders', $handler::class)
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $this->artisan('spoolrail:consume configured-orders')->run();

    $job = handlerPolicyJob();

    // --- Assert ---
    expect($job->tries)->toBe(5);
    expect($handler::$constructions)->toBe(0);
});

test('runs message-specific middleware in order around a handler constructed only for execution', function (): void {
    // --- Arrange ---
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
        'handle',
        'after:first',
    ]);
});

test('gives captured middleware the underlying Laravel Queue job', function (): void {
    // --- Arrange ---
    createHandlerPolicyJobsTable();

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
        ->onQueueConnection('database');
    Spoolrail::publish('orders', Message::make('order.created', []));
    $this->artisan('spoolrail:consume release-orders')->run();

    // --- Act ---
    $this->artisan('queue:work database --once --tries=1')->run();

    $queued = DB::connection('testing')->table('jobs')->first();

    // --- Assert ---
    expect($handler->handled)->toBeFalse();
    expect($queued->available_at)->toBeGreaterThan($queued->created_at);
});

test('keeps policy extraction failure inside the source handoff boundary', function (): void {
    // --- Arrange ---
    $handler = new class implements MessageHandler
    {
        public static bool $shouldFail = true;

        public ?string $handledMessage = null;

        public function handle(Message $message): void
        {
            $this->handledMessage = $message->id;
        }

        public function tries(): int
        {
            if (self::$shouldFail) {
                throw new RuntimeException('Handler queue policy failed.');
            }

            return 3;
        }
    };
    $handler::$shouldFail = true;
    app()->instance($handler::class, $handler);

    Spoolrail::subscribe('orders', 'failing-policy-orders', $handler::class);
    $published = Spoolrail::publish('orders', Message::make('order.created', []));

    // --- Act ---
    $failure = null;

    try {
        $this->artisan('spoolrail:consume failing-policy-orders')->run();
    } catch (Throwable $exception) {
        $failure = $exception;
    }

    $handler::$shouldFail = false;
    $this->artisan('spoolrail:consume failing-policy-orders')->run();

    // --- Assert ---
    expect($failure)->toBeInstanceOf(RuntimeException::class);
    expect($failure?->getMessage())->toBe('Handler queue policy failed.');
    expect($handler->handledMessage)->toBe($published->id);
});

test('uses captured policy while resolving a replacement handler at execution', function (): void {
    // --- Arrange ---
    createHandlerPolicyJobsTable();

    $original = new class implements MessageHandler
    {
        public function handle(Message $message): void {}

        public function tries(): int
        {
            return 5;
        }
    };

    Spoolrail::subscribe('orders', 'warehouse-orders', $original::class)
        ->onQueueConnection('database');
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

    $jobAfterDeployment = handlerPolicyJob();

    // --- Act ---
    $this->artisan('queue:work database --once')->run();

    // --- Assert ---
    expect($jobAfterDeployment->tries)->toBe(5);
    expect($replacement->handled)->toBeTrue();
});

function createHandlerPolicyJobsTable(): void
{
    Schema::connection('testing')->create('jobs', function (Blueprint $blueprint): void {
        $blueprint->id();
        $blueprint->string('queue')->index();
        $blueprint->longText('payload');
        $blueprint->unsignedTinyInteger('attempts');
        $blueprint->unsignedInteger('reserved_at')->nullable();
        $blueprint->unsignedInteger('available_at');
        $blueprint->unsignedInteger('created_at');
    });

}

function handlerPolicyJob(): HandleMessageJob
{
    $payload = DB::connection('testing')->table('jobs')->value('payload');
    $decoded = json_decode((string) $payload, true, flags: JSON_THROW_ON_ERROR);

    return unserialize($decoded['data']['command']);
}
