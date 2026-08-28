<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Support\Facades\Event;
use Spoolrail\Spoolrail\Contracts\CanClose;
use Spoolrail\Spoolrail\Contracts\CanWaitForConsumerIo;
use Spoolrail\Spoolrail\Contracts\Driver;
use Spoolrail\Spoolrail\Delivery;
use Spoolrail\Spoolrail\Exceptions\InvalidMessageEnvelopeException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\MessageEnvelope;
use Spoolrail\Spoolrail\Subscriptions\SubscriptionConsumer;
use Spoolrail\Spoolrail\Tests\Concerns\RecordsConsumerFailures;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(RecordsConsumerFailures::class);

beforeEach(function (): void {
    config()->set('queue.default', 'sync');
    config()->set('spoolrail.connections.runtime', ['driver' => 'runtime']);
    RecordingMessageHandler::reset();
});

test('overlaps receives and schedules one delivery per ready lane turn', function (): void {
    // --- Arrange ---
    $driver = new RuntimeDriver;
    $driver->batches = [
        'warehouse-orders' => [[
            runtimeDelivery('warehouse-1'),
            runtimeDelivery('warehouse-2'),
        ]],
        'billing-orders' => [[
            runtimeDelivery('billing-1'),
            runtimeDelivery('billing-2'),
        ]],
    ];
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(SubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        if (count(RecordingMessageHandler::$messages) === 4) {
            $consumer->stop();
        }
    });

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    expect($driver->maximumPendingReceives)->toBe(2);
    expect(array_map(
        static fn (Message $message): string => $message->payload['reference'],
        RecordingMessageHandler::$messages,
    ))->toBe([
        'warehouse-1',
        'billing-1',
        'warehouse-2',
        'billing-2',
    ]);
    expect($driver->acknowledged)->toBe([
        'warehouse-1',
        'billing-1',
        'warehouse-2',
        'billing-2',
    ]);
});

test("releases a failed lane's batch while a healthy sibling continues", function (): void {
    // --- Arrange ---
    $driver = new RuntimeDriver;
    $driver->batches = [
        'warehouse-orders' => [[
            new Delivery('not a message envelope', 'warehouse-invalid'),
            runtimeDelivery('warehouse-tail'),
        ]],
        'billing-orders' => [[
            runtimeDelivery('billing-1'),
            runtimeDelivery('billing-2'),
        ]],
    ];
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(SubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        if (count(RecordingMessageHandler::$messages) === 2) {
            $consumer->stop();
        }
    });

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    expect(array_map(
        static fn (Message $message): string => $message->payload['reference'],
        RecordingMessageHandler::$messages,
    ))->toBe(['billing-1', 'billing-2']);
    expect($driver->released)->toBe(['warehouse-invalid', 'warehouse-tail']);
    expect($driver->acknowledged)->toBe(['billing-1', 'billing-2']);
    expect($this->consumerFailures)->toHaveCount(1);
    expect($this->consumerFailures[0]->getPrevious())
        ->toBeInstanceOf(InvalidMessageEnvelopeException::class);
});

test("starts a lane's next receive only after its current batch settles", function (): void {
    // --- Arrange ---
    $driver = new RuntimeDriver;
    $driver->batches = [
        'warehouse-orders' => [
            [runtimeDelivery('warehouse-1'), runtimeDelivery('warehouse-2')],
            [runtimeDelivery('warehouse-3')],
        ],
        'billing-orders' => [[runtimeDelivery('billing-1')]],
    ];
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(SubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        if (count(RecordingMessageHandler::$messages) === 4) {
            $consumer->stop();
        }
    });

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    $warehouseReceives = array_keys(
        $driver->events,
        'receive:warehouse-orders',
        true,
    );
    expect($warehouseReceives)->toHaveCount(2);

    $secondReceiveAt = $warehouseReceives[1];
    $batchSettledAt = array_search(
        'acknowledged:warehouse-2',
        $driver->events,
        true,
    );

    expect($batchSettledAt)->toBeInt();
    expect($secondReceiveAt)->toBeGreaterThan($batchSettledAt);
    expect($driver->acknowledged)->toContain('warehouse-3');
});

test('retries a failed receive after backoff while a healthy sibling continues', function (): void {
    // --- Arrange ---
    $failure = new RuntimeException('Warehouse receive failed.');
    $driver = new RuntimeDriver;
    $driver->receiveFailures['warehouse-orders'] = [$failure];
    $driver->batches['warehouse-orders'] = [[
        runtimeDelivery('warehouse-recovered'),
    ]];
    $driver->batches['billing-orders'] = [[
        runtimeDelivery('billing-1'),
        runtimeDelivery('billing-2'),
    ]];
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(AdvancingSubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        if (count(RecordingMessageHandler::$messages) === 3) {
            $consumer->stop();
        }
    });

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    expect(array_map(
        static fn (Message $message): string => $message->payload['reference'],
        RecordingMessageHandler::$messages,
    ))->toBe(['billing-1', 'warehouse-recovered', 'billing-2']);
    expect($driver->acknowledged)->toBe([
        'billing-1',
        'warehouse-recovered',
        'billing-2',
    ]);
    expect($driver->released)->toBe([]);
    expect(array_keys($driver->events, 'receive:warehouse-orders', true))->toHaveCount(2);
    expect($this->consumerFailures)->toHaveCount(1);
    expect($this->consumerFailures[0]->getPrevious())->toBe($failure);
});

test('leaves an uncertain acknowledgment unsettled while releasing its batch tail', function (): void {
    // --- Arrange ---
    $failure = new RuntimeException('Warehouse acknowledgment failed.');
    $driver = new RuntimeDriver;
    $driver->batches = [
        'warehouse-orders' => [[
            runtimeDelivery('warehouse-1'),
            runtimeDelivery('warehouse-2'),
        ]],
        'billing-orders' => [[
            runtimeDelivery('billing-1'),
            runtimeDelivery('billing-2'),
        ]],
    ];
    $driver->acknowledgmentFailures['warehouse-1'] = $failure;
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(SubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        if (count(RecordingMessageHandler::$messages) === 3) {
            $consumer->stop();
        }
    });

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    expect($driver->acknowledged)->toBe(['billing-1', 'billing-2']);
    expect($driver->released)->toBe(['warehouse-2']);
    expect($driver->events)->toContain('acknowledge:warehouse-1');
    expect($driver->events)->not->toContain('release:warehouse-1');
    expect($this->consumerFailures)->toHaveCount(1);
    expect($this->consumerFailures[0]->getPrevious())->toBe($failure);
});

test('continues releasing a failed batch after one release fails', function (): void {
    // --- Arrange ---
    $releaseFailure = new RuntimeException('Warehouse release failed.');
    $driver = new RuntimeDriver;
    $driver->batches = [
        'warehouse-orders' => [[
            new Delivery('not a message envelope', 'warehouse-invalid'),
            runtimeDelivery('warehouse-tail'),
        ]],
        'billing-orders' => [[runtimeDelivery('billing-1')]],
    ];
    $driver->releaseFailures['warehouse-invalid'] = $releaseFailure;
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(SubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        $consumer->stop();
    });

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    expect($driver->acknowledged)->toBe(['billing-1']);
    expect($driver->events)->toContain(
        'release:warehouse-invalid',
        'release:warehouse-tail',
    );
    expect($driver->released)->toBe(['warehouse-tail']);
    expect($this->consumerFailures)->toHaveCount(2);
    expect($this->consumerFailures[1]->getPrevious())->toBe($releaseFailure);
});

test('lets a shared reactor failure terminate the grouped consumer', function (): void {
    // --- Arrange ---
    $driver = new RuntimeDriver;
    $driver->reactorFailure = new RuntimeException('Shared reactor failed.');
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(SubscriptionConsumer::class);

    // --- Act / Assert ---
    expect(fn () => $consumer->consume(['warehouse-orders', 'billing-orders']))
        ->toThrow(RuntimeException::class, 'Shared reactor failed.');
    expect($this->consumerFailures)->toBe([]);
});

test('releases a batch returned after shutdown while draining prior work', function (): void {
    // --- Arrange ---
    $driver = new RuntimeDriver;
    $driver->batches = [
        'warehouse-orders' => [[
            runtimeDelivery('warehouse-1'),
            runtimeDelivery('warehouse-2'),
        ]],
        'billing-orders' => [[runtimeDelivery('billing-late')]],
    ];
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(SubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        $consumer->stop();
    });

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    expect(array_map(
        static fn (Message $message): string => $message->payload['reference'],
        RecordingMessageHandler::$messages,
    ))->toBe(['warehouse-1', 'warehouse-2']);
    expect($driver->acknowledged)->toBe(['warehouse-1', 'warehouse-2']);
    expect($driver->released)->toBe(['billing-late']);
    expect($driver->closed)->toBeTrue();
});

test('closes a pending receive after buffered work drains during shutdown', function (): void {
    // --- Arrange ---
    $driver = new RuntimeDriver;
    $driver->settlementsBeforeReceives = true;
    $driver->batches = [
        'warehouse-orders' => [[runtimeDelivery('warehouse-1')]],
        'billing-orders' => [[runtimeDelivery('billing-late')]],
    ];
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(SubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        $consumer->stop();
    });

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    expect($driver->acknowledged)->toBe(['warehouse-1']);
    expect($driver->released)->toBe([]);
    expect($driver->cancelledReceives)->toBe(['billing-orders']);
    expect($driver->closed)->toBeTrue();
});

test('processes synchronous driver lanes through the same scheduler', function (): void {
    // --- Arrange ---
    $driver = new SynchronousRuntimeDriver;
    $driver->batches = [
        'warehouse-orders' => [[runtimeDelivery('warehouse-1')]],
        'billing-orders' => [[runtimeDelivery('billing-1')]],
    ];
    registerRuntimeDriver($driver);
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    Spoolrail::subscribe('orders', 'billing-orders', RecordingMessageHandler::class)
        ->onConnection('runtime');
    $consumer = app(SubscriptionConsumer::class);
    Event::listen(JobProcessed::class, static function () use ($consumer): void {
        if (count(RecordingMessageHandler::$messages) === 2) {
            $consumer->stop();
        }
    });

    // --- Act ---
    $consumer->consume(['warehouse-orders', 'billing-orders']);

    // --- Assert ---
    expect(array_map(
        static fn (Message $message): string => $message->payload['reference'],
        RecordingMessageHandler::$messages,
    ))->toBe(['warehouse-1', 'billing-1']);
    expect($driver->acknowledged)->toBe(['warehouse-1', 'billing-1']);
});

/** @implements Driver<string> */
class RuntimeDriver implements CanClose, CanWaitForConsumerIo, Driver
{
    /** @var array<string, list<list<Delivery<string>>>> */
    public array $batches = [];

    /** @var list<string> */
    public array $acknowledged = [];

    /** @var list<string> */
    public array $released = [];

    /** @var list<string> */
    public array $events = [];

    /** @var array<string, list<Throwable>> */
    public array $receiveFailures = [];

    /** @var array<string, Throwable> */
    public array $acknowledgmentFailures = [];

    /** @var array<string, Throwable> */
    public array $releaseFailures = [];

    /** @var list<string> */
    public array $cancelledReceives = [];

    public int $maximumPendingReceives = 0;

    public bool $closed = false;

    public bool $settlementsBeforeReceives = false;

    public ?Throwable $reactorFailure = null;

    /** @var array<string, array{Closure, Closure}> */
    private array $receives = [];

    /** @var list<string|Closure> */
    private array $operations = [];

    public function publish(
        string $topic,
        string $body,
        array $headers,
        ?string $orderingKey = null,
    ): void {}

    public function receive(
        string $subscription,
        Closure $received,
        Closure $fail,
    ): void {
        $this->events[] = "receive:$subscription";
        $this->receives[$subscription] = [$received, $fail];
        $this->operations[] = $subscription;
        $this->maximumPendingReceives = max(
            $this->maximumPendingReceives,
            count($this->receives),
        );
    }

    public function acknowledge(
        Delivery $delivery,
        Closure $acknowledged,
        Closure $fail,
    ): void {
        $receipt = $delivery->receipt;
        $this->events[] = "acknowledge:$receipt";
        $this->operations[] = function () use ($receipt, $acknowledged, $fail): void {
            if (($exception = $this->acknowledgmentFailures[$receipt] ?? null) instanceof Throwable) {
                unset($this->acknowledgmentFailures[$receipt]);
                $fail($exception);

                return;
            }

            $this->events[] = "acknowledged:$receipt";
            $this->acknowledged[] = $receipt;
            $acknowledged();
        };
    }

    public function release(
        Delivery $delivery,
        Closure $released,
        Closure $fail,
    ): void {
        $receipt = $delivery->receipt;
        $this->events[] = "release:$receipt";
        $this->operations[] = function () use ($receipt, $released, $fail): void {
            if (($exception = $this->releaseFailures[$receipt] ?? null) instanceof Throwable) {
                unset($this->releaseFailures[$receipt]);
                $fail($exception);

                return;
            }

            $this->events[] = "released:$receipt";
            $this->released[] = $receipt;
            $released();
        };
    }

    public function waitForConsumerIo(): void
    {
        if ($this->reactorFailure instanceof Throwable) {
            throw $this->reactorFailure;
        }

        if ($this->settlementsBeforeReceives) {
            foreach ($this->operations as $index => $operation) {
                if (! $operation instanceof Closure) {
                    continue;
                }

                unset($this->operations[$index]);
                $this->operations = array_values($this->operations);
                $operation();

                return;
            }
        }

        $operation = array_shift($this->operations);

        if ($operation instanceof Closure) {
            $operation();

            return;
        }

        if (! is_string($operation)) {
            return;
        }

        [$received, $fail] = $this->receives[$operation];
        unset($this->receives[$operation]);

        $failures = $this->receiveFailures[$operation] ?? [];

        if (($exception = array_shift($failures)) instanceof Throwable) {
            $this->receiveFailures[$operation] = $failures;
            $fail($exception);

            return;
        }

        $received(array_shift($this->batches[$operation]) ?? []);
    }

    public function close(): void
    {
        $this->closed = true;
        $this->cancelledReceives = array_keys($this->receives);
        $this->receives = [];
        $this->operations = [];
    }
}

/** @implements Driver<string> */
class SynchronousRuntimeDriver implements CanClose, Driver
{
    /** @var array<string, list<list<Delivery<string>>>> */
    public array $batches = [];

    /** @var list<string> */
    public array $acknowledged = [];

    public bool $closed = false;

    public function publish(
        string $topic,
        string $body,
        array $headers,
        ?string $orderingKey = null,
    ): void {}

    public function receive(
        string $subscription,
        Closure $received,
        Closure $fail,
    ): void {
        $received(array_shift($this->batches[$subscription]) ?? []);
    }

    public function acknowledge(
        Delivery $delivery,
        Closure $acknowledged,
        Closure $fail,
    ): void {
        $this->acknowledged[] = $delivery->receipt;
        $acknowledged();
    }

    public function release(
        Delivery $delivery,
        Closure $released,
        Closure $fail,
    ): void {
        $released();
    }

    public function close(): void
    {
        $this->closed = true;
    }
}

class AdvancingSubscriptionConsumer extends SubscriptionConsumer
{
    private float $time = 0;

    #[Override]
    protected function now(): float
    {
        return $this->time++;
    }
}

/** @param  Driver<covariant mixed>  $driver */
function registerRuntimeDriver(Driver $driver): void
{
    Spoolrail::extend(
        'runtime',
        static fn (Application $_app, array $_config, string $_connection): Driver => $driver,
    );
}

/** @return Delivery<string> */
function runtimeDelivery(string $reference): Delivery
{
    $message = Message::make('order.created', ['reference' => $reference])
        ->withPublishedAt(CarbonImmutable::parse('2026-08-28T12:00:00Z'));

    return new Delivery(
        body: (new MessageEnvelope)->encode($message),
        receipt: $reference,
    );
}
