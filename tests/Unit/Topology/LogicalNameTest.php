<?php

declare(strict_types=1);

use Spoolrail\Spoolrail\Topology\LogicalName;

test('accepts the portable logical-name grammar', function (string $name): void {
    expect(LogicalName::isValid($name))->toBeTrue();
})->with([
    'three-character boundary' => 'Ab3',
    'hyphen and underscore' => 'A-_',
    'mixed ASCII characters' => 'Orders_2026-live',
]);

test('rejects values outside the portable logical-name grammar', function (string $name): void {
    expect(LogicalName::isValid($name))->toBeFalse();
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
