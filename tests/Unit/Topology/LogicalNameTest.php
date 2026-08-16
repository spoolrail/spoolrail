<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Topology\LogicalName;

test('accepts the portable logical-name grammar', function (string $name): void {
    expect(LogicalName::isValidTopic($name))->toBeTrue()
        ->and(LogicalName::isValidSubscription($name))->toBeTrue();
})->with([
    'three-character boundary' => 'Ab3',
    'hyphen and underscore' => 'A-_',
    'mixed ASCII characters' => 'Orders_2026-live',
]);

test('rejects values outside the portable logical-name grammar', function (string $name): void {
    expect(LogicalName::isValidTopic($name))->toBeFalse()
        ->and(LogicalName::isValidSubscription($name))->toBeFalse();
})->with([
    'blank' => '   ',
    'short' => 'ab',
    'digit-leading' => '1orders',
    'punctuation' => 'orders.created',
    'non-ASCII leading character' => 'Évents',
    'non-ASCII body character' => 'ordérs',
    'leading whitespace' => ' orders',
    'trailing whitespace' => 'orders ',
    'trailing newline' => "orders\n",
]);

test('applies the portable length budget for each logical-name role', function (): void {
    $topicAtLimit = 't'.str_repeat('o', 250);
    $subscriptionAtLimit = 's'.str_repeat('u', 49);

    expect(LogicalName::isValidTopic($topicAtLimit))->toBeTrue()
        ->and(LogicalName::isValidTopic("{$topicAtLimit}o"))->toBeFalse()
        ->and(LogicalName::isValidSubscription($subscriptionAtLimit))->toBeTrue()
        ->and(LogicalName::isValidSubscription("{$subscriptionAtLimit}u"))->toBeFalse();
});

test('reserves transport-specific topic beginnings without restricting subscription names', function (): void {
    expect(LogicalName::isValidTopic('GoOg-orders'))->toBeFalse()
        ->and(LogicalName::isValidSubscription('GoOg-orders'))->toBeTrue();
});
