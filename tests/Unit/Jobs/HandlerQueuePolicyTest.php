<?php

declare(strict_types=1);

use Carbon\CarbonImmutable;
use Illuminate\Queue\Attributes\Backoff;
use Illuminate\Queue\Attributes\FailOnTimeout;
use Illuminate\Queue\Attributes\MaxExceptions;
use Illuminate\Queue\Attributes\Timeout;
use Illuminate\Queue\Attributes\Tries;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Jobs\HandleMessageJob;
use Spoolrail\Spoolrail\Jobs\HandlerQueuePolicy;
use Spoolrail\Spoolrail\Message;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

beforeEach(function (): void {
    RecordingMessageHandler::reset();
});

test('uses property fallbacks and the retry deadline method', function (): void {
    // --- Arrange ---
    $handler = new class implements MessageHandler
    {
        public int $tries = 3;

        /** @var list<int> */
        public array $backoff = [5, 10];

        public function handle(Message $message): void {}

        public function retryUntil(): DateTimeInterface
        {
            return CarbonImmutable::parse('2030-02-03 04:05:06 UTC');
        }
    };
    $message = Message::make('order.created', []);
    $job = new HandleMessageJob($message, 'property-orders');

    // --- Act ---
    (new HandlerQueuePolicy)->apply($handler::class, $message, $job);

    // --- Assert ---
    expect($job->tries)->toBe(3);
    expect($job->backoff)->toBe([5, 10]);
    expect($job->failOnTimeout)->toBeFalse();
    expect($job->retryUntil?->getTimestamp())->toBe(
        CarbonImmutable::parse('2030-02-03 04:05:06 UTC')->getTimestamp(),
    );
});

test('uses Laravel Queue attributes before same-class default properties', function (): void {
    // --- Arrange ---
    $handler = new #[Tries(5), Backoff(20), MaxExceptions(5), Timeout(120), FailOnTimeout] class implements MessageHandler
    {
        public int $tries = 3;

        public int $backoff = 10;

        public int $maxExceptions = 2;

        public int $timeout = 90;

        public bool $failOnTimeout = false;

        public function handle(Message $message): void {}
    };
    $message = Message::make('order.created', []);
    $job = new HandleMessageJob($message, 'attribute-orders');

    // --- Act ---
    (new HandlerQueuePolicy)->apply($handler::class, $message, $job);

    // --- Assert ---
    expect($job->tries)->toBe(5);
    expect($job->backoff)->toBe(20);
    expect($job->maxExceptions)->toBe(5);
    expect($job->timeout)->toBe(120);
    expect($job->failOnTimeout)->toBeTrue();
})->skip(! class_exists(Tries::class), 'Laravel Queue policy attributes require Laravel 13.');

test('ignores a retry deadline property', function (): void {
    // --- Arrange ---
    $handler = new class implements MessageHandler
    {
        public int $retryUntil = 123;

        public function handle(Message $message): void {}
    };
    $message = Message::make('order.created', []);
    $job = new HandleMessageJob($message, 'property-deadline-orders');

    // --- Act ---
    (new HandlerQueuePolicy)->apply($handler::class, $message, $job);

    // --- Assert ---
    expect($job->retryUntil)->toBeNull();
});

test('uses methods before properties', function (): void {
    // --- Arrange ---
    $handler = new class implements MessageHandler
    {
        public int $tries = 3;

        public int $backoff = 10;

        public function handle(Message $message): void {}

        public function tries(): int
        {
            return 7;
        }

        /** @return list<int> */
        public function backoff(): array
        {
            return [11, 22];
        }
    };
    $message = Message::make('order.created', []);
    $job = new HandleMessageJob($message, 'method-property-orders');

    // --- Act ---
    (new HandlerQueuePolicy)->apply($handler::class, $message, $job);

    // --- Assert ---
    expect($job->tries)->toBe(7);
    expect($job->backoff)->toBe([11, 22]);
});

test('uses methods before attributes', function (): void {
    // --- Arrange ---
    $handler = new #[Tries(5), Backoff(20)] class implements MessageHandler
    {
        public function handle(Message $message): void {}

        public function tries(): int
        {
            return 7;
        }

        /** @return list<int> */
        public function backoff(): array
        {
            return [11, 22];
        }
    };
    $message = Message::make('order.created', []);
    $job = new HandleMessageJob($message, 'method-attribute-orders');

    // --- Act ---
    (new HandlerQueuePolicy)->apply($handler::class, $message, $job);

    // --- Assert ---
    expect($job->tries)->toBe(7);
    expect($job->backoff)->toBe([11, 22]);
})->skip(! class_exists(Tries::class), 'Laravel Queue policy attributes require Laravel 13.');

test('uses a child public property before an inherited attribute', function (): void {
    // --- Arrange ---
    $handler = new class extends RecordingMessageHandler
    {
        public int $maxExceptions = 8;
    };
    $message = Message::make('order.created', []);
    $job = new HandleMessageJob($message, 'child-orders');

    // --- Act ---
    (new HandlerQueuePolicy)->apply($handler::class, $message, $job);

    // --- Assert ---
    expect($job->maxExceptions)->toBe(8);
})->skip(! class_exists(MaxExceptions::class), 'Laravel Queue policy attributes require Laravel 13.');

test('uses an attribute declared by the handler trait before its default property', function (): void {
    // --- Arrange ---
    $message = Message::make('order.created', []);
    $job = new HandleMessageJob($message, 'trait-orders');

    // --- Act ---
    (new HandlerQueuePolicy)->apply(RecordingMessageHandler::class, $message, $job);

    // --- Assert ---
    expect($job->timeout)->toBe(75);
})->skip(! class_exists(Timeout::class), 'Laravel Queue policy attributes require Laravel 13.');
