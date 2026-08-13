<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\SnsSqs\ConnectionConfig;
use Spoolrail\Spoolrail\SnsSqs\ResourceName;

test('derives standard and FIFO AWS resource names', function (): void {
    expect(ResourceName::topic('orders', false))->toBe('orders');
    expect(ResourceName::topic('orders', true))->toBe('orders.fifo');
    expect(ResourceName::queue('warehouse', 'orders', false))->toBe('warehouse-orders');
    expect(ResourceName::queue('warehouse', 'orders', true))->toBe('warehouse-orders.fifo');
});

test('fits portable logical names at the AWS transport limits', function (): void {
    $topic = 't'.str_repeat('o', 250);
    $prefix = 'p'.str_repeat('r', 23);
    $subscription = 's'.str_repeat('u', 49);
    $physicalTopic = ResourceName::topic($topic, true);
    $physicalQueue = ResourceName::queue($prefix, $subscription, true);

    expect($physicalTopic)->toBe("$topic.fifo");
    expect(strlen($physicalTopic))->toBe(256);
    expect($physicalQueue)->toBe("$prefix-$subscription.fifo");
    expect(strlen($physicalQueue))->toBe(80);
});

test('derives partition region and owner aware ARNs', function (string $region, string $partition): void {
    $config = new ConnectionConfig('snssqs', [
        'region' => $region,
        'account_id' => '123456789012',
        'fifo' => true,
    ]);

    expect(ResourceName::topicArn($config, 'orders'))
        ->toBe("arn:$partition:sns:$region:123456789012:orders.fifo");
    expect(ResourceName::queueArn($config, 'warehouse-orders.fifo'))
        ->toBe("arn:$partition:sqs:$region:123456789012:warehouse-orders.fifo");
})->with([
    'commercial' => ['us-east-1', 'aws'],
    'China' => ['cn-north-1', 'aws-cn'],
    'GovCloud' => ['us-gov-west-1', 'aws-us-gov'],
]);
