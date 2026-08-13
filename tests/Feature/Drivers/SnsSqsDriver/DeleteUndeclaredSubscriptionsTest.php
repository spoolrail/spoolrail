<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Facades\Spoolrail;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithSnsSqs;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

uses(InteractsWithSnsSqs::class);

test('deletes only undeclared AWS receive resources under a retired ownership prefix', function (): void {
    // --- Arrange ---
    Spoolrail::subscribe('orders', 'current-orders', RecordingMessageHandler::class)
        ->onConnection('snssqs');
    $this->artisan('spoolrail:sync')->run();

    $retiredSubscription = new Subscription(
        'orders',
        'old-orders',
        RecordingMessageHandler::class,
        static function (): void {},
    );
    Spoolrail::connection('snssqs')->topology()?->planSync(
        [$retiredSubscription],
        'retired-application',
    )->apply();

    // --- Act ---
    $exitCode = $this->artisan(
        'spoolrail:delete-undeclared-subscriptions --connection=snssqs --retired-prefix=retired-application',
    )
        ->expectsOutputToContain('Deleted subscription resource [retired-application-old-orders.fifo].')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($this->sqsQueueExists('current-orders'))->toBeTrue();
    expect($this->sqsQueueExists(
        'old-orders',
        ownershipPrefix: 'retired-application',
    ))->toBeFalse();

    $endpoints = array_column(
        $this->sns->listSubscriptionsByTopic([
            'TopicArn' => $this->snsTopicArn('orders'),
        ])->get('Subscriptions') ?? [],
        'Endpoint',
    );
    expect($endpoints)->not->toContain(
        "arn:aws:sqs:$this->awsRegion:$this->awsAccountId:retired-application-old-orders.fifo",
    );
});
