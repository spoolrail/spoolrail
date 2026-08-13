<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Exceptions\SnsSqsTopologyException;
use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithSnsSqs;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithSnsSqs::class);

test('deletes an AWS topic only after its receive route has been explicitly retired', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'warehouse-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');
    $this->artisan('spoolrail:sync')->run();

    // --- Act ---
    $whileSubscribed = fn () => $this->artisan(
        'spoolrail:delete-topic orders --connection=snssqs',
    )->run();

    // --- Assert ---
    expect($whileSubscribed)->toThrow(SnsSqsTopologyException::class, 'while it has subscriptions');

    Spoolrail::connection('snssqs')->topology()?->deleteSubscription(
        $this->sqsQueueName('warehouse-orders'),
    );

    $exitCode = $this->artisan(
        'spoolrail:delete-topic orders --connection=snssqs',
    )
        ->expectsOutputToContain('Deleted topic [orders] from connection [snssqs].')
        ->run();

    expect($exitCode)->toBe(0);
    expect($this->sqsQueueExists('warehouse-orders'))->toBeFalse();
    expect($this->snsTopicExists('orders'))->toBeFalse();
});
