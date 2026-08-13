<?php

declare(strict_types=1);

use Aws\Exception\AwsException;
use Aws\MockHandler;
use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use GuzzleHttp\Psr7\Response;
use Spoolrail\Spoolrail\Exceptions\SnsSqsTopologyException;
use Spoolrail\Spoolrail\Exceptions\TopologySyncRequiresRetryException;
use Spoolrail\Spoolrail\SnsSqs\PendingTopology;

test('requests a topology retry when an AWS apply result is ambiguous', function (): void {
    // --- Arrange ---
    $commandClient = new SnsClient(pendingTopologyClientOptions());
    $failure = new AwsException(
        'Service unavailable after dispatch.',
        $commandClient->getCommand('CreateTopic'),
        ['response' => new Response(503)],
    );
    $sns = new SnsClient(pendingTopologyClientOptions(new MockHandler([$failure])));
    $plan = new PendingTopology(
        $sns,
        new SqsClient(pendingTopologyClientOptions(new MockHandler)),
    );
    $plan->addTopic(
        'orders.fifo',
        'arn:aws:sns:us-east-1:123456789012:orders.fifo',
        ['FifoTopic' => 'true'],
    );

    // --- Act ---
    $caught = null;

    try {
        $plan->apply();
    } catch (TopologySyncRequiresRetryException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->getPrevious())->toBeInstanceOf(SnsSqsTopologyException::class);
    expect($caught?->getPrevious()?->getPrevious())->toBe($failure);
});

test('fails an AWS topology apply immediately after a permanent refusal', function (): void {
    // --- Arrange ---
    $commandClient = new SnsClient(pendingTopologyClientOptions());
    $failure = new AwsException(
        'Forbidden.',
        $commandClient->getCommand('CreateTopic'),
        ['response' => new Response(403)],
    );
    $sns = new SnsClient(pendingTopologyClientOptions(new MockHandler([$failure])));
    $plan = new PendingTopology(
        $sns,
        new SqsClient(pendingTopologyClientOptions(new MockHandler)),
    );
    $plan->addTopic(
        'orders.fifo',
        'arn:aws:sns:us-east-1:123456789012:orders.fifo',
        ['FifoTopic' => 'true'],
    );

    // --- Act ---
    $caught = null;

    try {
        $plan->apply();
    } catch (SnsSqsTopologyException $exception) {
        $caught = $exception;
    }

    // --- Assert ---
    expect($caught?->getPrevious())->toBe($failure);
    expect($caught?->getMessage())->toContain('creating SNS topic [orders.fifo]');
});

/**
 * @return array<string, mixed>
 */
function pendingTopologyClientOptions(?MockHandler $handler = null): array
{
    $options = [
        'version' => 'latest',
        'region' => 'us-east-1',
        'endpoint' => 'http://localhost:4566',
        'credentials' => false,
        'md5' => false,
        'retries' => 0,
    ];

    if ($handler instanceof MockHandler) {
        $options['handler'] = $handler;
    }

    return $options;
}
