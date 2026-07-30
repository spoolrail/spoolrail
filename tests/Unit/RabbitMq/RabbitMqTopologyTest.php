<?php

declare(strict_types=1);

use Illuminate\Http\Client\Factory;
use Illuminate\Http\Client\Request;
use Spoolrail\Spoolrail\Exceptions\RabbitMqManagementException;
use Spoolrail\Spoolrail\Exceptions\RabbitMqTopologyException;
use Spoolrail\Spoolrail\Exceptions\UnsupportedRabbitMqVersionException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqManagementClient;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqTopology;
use Spoolrail\Spoolrail\Subscriptions\Subscription;
use Spoolrail\Spoolrail\Tests\Fixtures\RecordingMessageHandler;

/**
 * @param  array<string, array{mixed, int}>  $responses
 * @return array{RabbitMqTopology, Factory}
 */
function rabbitMqTopology(array $responses = []): array
{
    $responses = array_replace([
        'GET overview' => [['rabbitmq_version' => '4.3.2'], 200],
        'GET vhosts/%2F' => [['default_queue_type' => 'classic'], 200],
        'GET policies/%2F' => [[], 200],
        'GET operator-policies/%2F' => [[], 200],
    ], $responses);
    $http = new Factory;

    $http->fake(function (Request $request) use ($http, $responses) {
        $path = parse_url($request->url(), PHP_URL_PATH);
        $apiPath = is_string($path) ? strstr($path, '/api/') : false;
        $key = $request->method().' '.($apiPath === false ? '' : substr($apiPath, 5));
        [$body, $status] = $responses[$key] ?? [[], $request->method() === 'GET' ? 404 : 204];

        return $http->response($body, $status);
    });

    $connectionConfig = new RabbitMqConnectionConfig('events', []);

    return [
        new RabbitMqTopology(
            $connectionConfig,
            new RabbitMqManagementClient($connectionConfig, $http),
        ),
        $http,
    ];
}

function rabbitMqSubscription(string $topic = 'orders', string $name = 'warehouse'): Subscription
{
    return new Subscription(
        $topic,
        $name,
        RecordingMessageHandler::class,
        static function (): void {},
    );
}

/**
 * @return array<string, array{mixed, int}>
 */
function compatibleQuorumTopologyResponses(array $arguments = []): array
{
    return [
        'GET exchanges/%2F/orders' => [[
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ], 200],
        'GET queues/%2F/application-a-warehouse' => [[
            'type' => 'quorum',
            'durable' => true,
            'exclusive' => false,
            'auto_delete' => false,
            'arguments' => $arguments,
        ], 200],
        'GET queues/%2F/application-a-warehouse/bindings' => [[
            [
                'source' => 'orders',
                'destination_type' => 'queue',
                'routing_key' => '',
                'arguments' => [],
            ],
        ], 200],
    ];
}

test('rejects a stream default queue type before creation', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology([
        'GET vhosts/%2F' => [['default_queue_type' => 'stream'], 200],
    ]);

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)
        ->toThrow(RabbitMqTopologyException::class, 'default queue type [stream]');
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() !== 'GET',
    );
});

test('accepts compatible existing topology when the virtual host defaults new queues to streams', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology([
        'GET vhosts/%2F' => [['default_queue_type' => 'stream'], 200],
        'GET exchanges/%2F/orders' => [[
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ], 200],
        'GET queues/%2F/application-a-warehouse' => [[
            'type' => 'classic',
            'durable' => true,
            'exclusive' => false,
            'auto_delete' => false,
            'arguments' => [],
        ], 200],
        'GET queues/%2F/application-a-warehouse/bindings' => [[
            [
                'source' => 'orders',
                'destination_type' => 'queue',
                'routing_key' => '',
                'arguments' => [],
            ],
        ], 200],
    ]);

    // --- Act ---
    $topology->planSync([rabbitMqSubscription()], 'application-a')->apply();

    // --- Assert ---
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() !== 'GET',
    );
});

test('rejects an unsupported broker before planning any mutations', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology([
        'GET overview' => [['rabbitmq_version' => '4.2.9'], 200],
    ]);

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(
        UnsupportedRabbitMqVersionException::class,
        'RabbitMQ [4.2.9] is not supported; Spoolrail requires RabbitMQ 4.3 or later.',
    );
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() !== 'GET',
    );
});

test('rejects broker metadata without a version before planning any mutations', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology([
        'GET overview' => [[], 200],
    ]);

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(
        RabbitMqManagementException::class,
        'RabbitMQ connection [events] Management API returned an invalid response while reading the broker version.',
    );
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() !== 'GET',
    );
});

test('rejects an existing transient queue', function (): void {
    // --- Arrange ---
    [$topology] = rabbitMqTopology([
        'GET exchanges/%2F/orders' => [[
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ], 200],
        'GET queues/%2F/application-a-warehouse' => [[
            'type' => 'classic',
            'durable' => false,
            'exclusive' => false,
            'auto_delete' => false,
            'arguments' => [],
        ], 200],
    ]);

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(RabbitMqTopologyException::class, '[durable] must be true');
});

test('rejects an existing queue with an unsupported type', function (): void {
    // --- Arrange ---
    [$topology] = rabbitMqTopology([
        'GET exchanges/%2F/orders' => [[
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ], 200],
        'GET queues/%2F/application-a-warehouse' => [[
            'type' => 'stream',
            'durable' => true,
            'exclusive' => false,
            'auto_delete' => false,
            'arguments' => [],
        ], 200],
    ]);

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(RabbitMqTopologyException::class, 'queue type must be classic or quorum');
});

test('rejects a wrong or additional queue binding', function (array $bindings): void {
    // --- Arrange ---
    [$topology] = rabbitMqTopology([
        'GET exchanges/%2F/orders' => [[
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ], 200],
        'GET queues/%2F/application-a-warehouse' => [[
            'type' => 'classic',
            'durable' => true,
            'exclusive' => false,
            'auto_delete' => false,
            'arguments' => [],
        ], 200],
        'GET queues/%2F/application-a-warehouse/bindings' => [$bindings, 200],
    ]);

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(RabbitMqTopologyException::class, 'must have only one non-default exchange binding');
})->with([
    'wrong topic' => [[
        [
            'source' => 'returns',
            'destination_type' => 'queue',
            'routing_key' => '',
            'arguments' => [],
        ],
    ]],
    'additional topic' => [[
        [
            'source' => 'orders',
            'destination_type' => 'queue',
            'routing_key' => '',
            'arguments' => [],
        ],
        [
            'source' => 'returns',
            'destination_type' => 'queue',
            'routing_key' => '',
            'arguments' => [],
        ],
    ]],
]);

test('accepts compatible existing classic and unlimited quorum queues', function (string $type, array $arguments): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology([
        'GET exchanges/%2F/orders' => [[
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ], 200],
        'GET queues/%2F/application-a-warehouse' => [[
            'type' => $type,
            'durable' => true,
            'exclusive' => false,
            'auto_delete' => false,
            'arguments' => $arguments,
        ], 200],
        'GET queues/%2F/application-a-warehouse/bindings' => [[
            [
                'source' => 'orders',
                'destination_type' => 'queue',
                'routing_key' => '',
                'arguments' => [],
            ],
        ], 200],
    ]);

    // --- Act ---
    $topology->planSync([rabbitMqSubscription()], 'application-a')->apply();

    // --- Assert ---
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() !== 'GET',
    );
})->with([
    'classic' => ['classic', []],
    'quorum with a declaration limit' => ['quorum', ['x-delivery-limit' => -1]],
]);

test('accepts an existing quorum queue with an unlimited regular policy', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology(array_replace(compatibleQuorumTopologyResponses(), [
        'GET policies/%2F' => [[
            [
                'name' => 'unlimited',
                'pattern' => '^application-a-',
                'apply-to' => 'quorum_queues',
                'priority' => 10,
                'definition' => ['delivery-limit' => -1],
            ],
        ], 200],
    ]));

    // --- Act ---
    $topology->planSync([rabbitMqSubscription()], 'application-a')->apply();

    // --- Assert ---
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() !== 'GET',
    );
});

test('rejects an existing quorum queue without a verifiable unlimited delivery limit', function (): void {
    // --- Arrange ---
    [$topology] = rabbitMqTopology(compatibleQuorumTopologyResponses());

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(
        RabbitMqTopologyException::class,
        'RabbitMQ quorum queue [application-a-warehouse] does not have a verifiable unlimited delivery limit.',
    );
});

test('rejects a finite regular or operator policy', function (string $endpoint): void {
    // --- Arrange ---
    [$topology] = rabbitMqTopology(array_replace(compatibleQuorumTopologyResponses(), [
        "GET $endpoint/%2F" => [[
            [
                'name' => 'finite',
                'pattern' => '^application-a-',
                'apply-to' => 'quorum_queues',
                'priority' => 10,
                'definition' => ['delivery-limit' => 20],
            ],
        ], 200],
    ]));

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(RabbitMqTopologyException::class, 'finite delivery limit of 20');
})->with([
    'regular policy' => ['policies'],
    'operator policy' => ['operator-policies'],
]);

test('rejects a finite delivery limit from any combined source', function (
    array $arguments,
    array $policies,
    array $operatorPolicies,
): void {
    // --- Arrange ---
    [$topology] = rabbitMqTopology(array_replace(compatibleQuorumTopologyResponses($arguments), [
        'GET policies/%2F' => [$policies, 200],
        'GET operator-policies/%2F' => [$operatorPolicies, 200],
    ]));

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(RabbitMqTopologyException::class, 'finite delivery limit of 20');
})->with([
    'regular policy with unlimited operator policy' => [
        [],
        [[
            'name' => 'finite',
            'pattern' => '^application-a-',
            'apply-to' => 'quorum_queues',
            'priority' => 10,
            'definition' => ['delivery-limit' => 20],
        ]],
        [[
            'name' => 'unlimited',
            'pattern' => '^application-a-',
            'apply-to' => 'quorum_queues',
            'priority' => 10,
            'definition' => ['delivery-limit' => -1],
        ]],
    ],
    'declaration with unlimited operator policy' => [
        ['x-delivery-limit' => 20],
        [],
        [[
            'name' => 'unlimited',
            'pattern' => '^application-a-',
            'apply-to' => 'quorum_queues',
            'priority' => 10,
            'definition' => ['delivery-limit' => -1],
        ]],
    ],
    'unlimited declaration with regular policy' => [
        ['x-delivery-limit' => -1],
        [[
            'name' => 'finite',
            'pattern' => '^application-a-',
            'apply-to' => 'quorum_queues',
            'priority' => 10,
            'definition' => ['delivery-limit' => 20],
        ]],
        [],
    ],
]);

test('rejects a finite regular policy before creating a missing quorum queue', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology([
        'GET vhosts/%2F' => [['default_queue_type' => 'quorum'], 200],
        'GET policies/%2F' => [[
            [
                'name' => 'finite',
                'pattern' => '^application-a-',
                'apply-to' => 'quorum_queues',
                'priority' => 10,
                'definition' => ['delivery-limit' => 20],
            ],
        ], 200],
    ]);

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(RabbitMqTopologyException::class, 'finite delivery limit of 20');
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() !== 'GET',
    );
});

test('accepts equal-priority policies only when every possible winner is unlimited', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology(array_replace(compatibleQuorumTopologyResponses(), [
        'GET policies/%2F' => [[
            [
                'name' => 'unlimited-a',
                'pattern' => '^application-a-',
                'apply-to' => 'quorum_queues',
                'priority' => 10,
                'definition' => ['delivery-limit' => -1],
            ],
            [
                'name' => 'unlimited-b',
                'pattern' => 'warehouse$',
                'apply-to' => 'queues',
                'priority' => 10,
                'definition' => ['delivery-limit' => -1],
            ],
        ], 200],
    ]));

    // --- Act ---
    $topology->planSync([rabbitMqSubscription()], 'application-a')->apply();

    // --- Assert ---
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() !== 'GET',
    );
});

test('rejects conflicting equal-priority policy winners', function (): void {
    // --- Arrange ---
    [$topology] = rabbitMqTopology(array_replace(compatibleQuorumTopologyResponses(), [
        'GET policies/%2F' => [[
            [
                'name' => 'unlimited',
                'pattern' => '^application-a-',
                'apply-to' => 'quorum_queues',
                'priority' => 10,
                'definition' => ['delivery-limit' => -1],
            ],
            [
                'name' => 'finite',
                'pattern' => 'warehouse$',
                'apply-to' => 'queues',
                'priority' => 10,
                'definition' => ['delivery-limit' => 20],
            ],
        ], 200],
    ]));

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(RabbitMqTopologyException::class, 'finite delivery limit of 20');
});

test('rejects a matching policy whose regular expression cannot be evaluated', function (): void {
    // --- Arrange ---
    [$topology] = rabbitMqTopology(array_replace(compatibleQuorumTopologyResponses([
        'x-delivery-limit' => -1,
    ]), [
        'GET policies/%2F' => [[
            [
                'name' => 'malformed',
                'pattern' => '[',
                'apply-to' => 'quorum_queues',
                'priority' => 10,
                'definition' => ['delivery-limit' => -1],
            ],
        ], 200],
    ]));

    // --- Act ---
    $action = fn () => $topology->planSync([rabbitMqSubscription()], 'application-a');

    // --- Assert ---
    expect($action)->toThrow(
        RabbitMqTopologyException::class,
        'RabbitMQ policy [malformed] cannot be evaluated safely.',
    );
});

test('creates a missing binding for an otherwise compatible existing queue', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology([
        'GET exchanges/%2F/orders' => [[
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ], 200],
        'GET queues/%2F/application-a-warehouse' => [[
            'type' => 'classic',
            'durable' => true,
            'exclusive' => false,
            'auto_delete' => false,
            'arguments' => [],
        ], 200],
        'GET queues/%2F/application-a-warehouse/bindings' => [[], 200],
    ]);

    // --- Act ---
    $topology->planSync([rabbitMqSubscription()], 'application-a')->apply();

    // --- Assert ---
    $http->assertSent(
        static fn (Request $request): bool => $request->method() === 'POST'
            && str_ends_with($request->url(), '/api/bindings/%2F/e/orders/q/application-a-warehouse'),
    );
});

test('shares one topic exchange while keeping application queue namespaces distinct', function (): void {
    // --- Arrange ---
    [$applicationA, $applicationAHttp] = rabbitMqTopology([
        'GET exchanges/%2F/orders' => [[], 404],
        'GET queues/%2F/application-a-warehouse' => [[], 404],
    ]);
    [$applicationB, $applicationBHttp] = rabbitMqTopology([
        'GET exchanges/%2F/orders' => [[
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ], 200],
        'GET queues/%2F/application-b-warehouse' => [[], 404],
    ]);

    // --- Act ---
    $applicationA->planSync([rabbitMqSubscription()], 'application-a')->apply();
    $applicationB->planSync([rabbitMqSubscription()], 'application-b')->apply();

    // --- Assert ---
    $applicationAHttp->assertSent(
        static fn (Request $request): bool => $request->method() === 'PUT'
            && str_ends_with($request->url(), '/api/exchanges/%2F/orders'),
    );
    $applicationAHttp->assertSent(
        static fn (Request $request): bool => $request->method() === 'PUT'
            && str_ends_with($request->url(), '/api/queues/%2F/application-a-warehouse'),
    );
    $applicationBHttp->assertNotSent(
        static fn (Request $request): bool => $request->method() === 'PUT'
            && str_ends_with($request->url(), '/api/exchanges/%2F/orders'),
    );
    $applicationBHttp->assertSent(
        static fn (Request $request): bool => $request->method() === 'PUT'
            && str_ends_with($request->url(), '/api/queues/%2F/application-b-warehouse'),
    );
    $applicationAHttp->assertSent(
        static fn (Request $request): bool => $request->method() === 'POST'
            && str_ends_with($request->url(), '/api/bindings/%2F/e/orders/q/application-a-warehouse'),
    );
    $applicationBHttp->assertSent(
        static fn (Request $request): bool => $request->method() === 'POST'
            && str_ends_with($request->url(), '/api/bindings/%2F/e/orders/q/application-b-warehouse'),
    );
});

test('refuses to delete a missing topic', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology([
        'GET exchanges/%2F/orders' => [[], 404],
    ]);

    // --- Act ---
    $action = fn () => $topology->deleteTopic('orders');

    // --- Assert ---
    expect($action)->toThrow(RabbitMqTopologyException::class, 'does not exist');
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() === 'DELETE',
    );
});

test('refuses to delete a topic with an incoming exchange binding', function (): void {
    // --- Arrange ---
    [$topology, $http] = rabbitMqTopology([
        'GET exchanges/%2F/orders' => [[
            'type' => 'fanout',
            'durable' => true,
            'auto_delete' => false,
            'internal' => false,
        ], 200],
        'GET exchanges/%2F/orders/bindings/source' => [[], 200],
        'GET exchanges/%2F/orders/bindings/destination' => [[
            [
                'source' => 'all-events',
                'destination' => 'orders',
                'destination_type' => 'exchange',
            ],
        ], 200],
    ]);

    // --- Act ---
    $action = fn () => $topology->deleteTopic('orders');

    // --- Assert ---
    expect($action)->toThrow(RabbitMqTopologyException::class, 'while it has bindings');
    $http->assertNotSent(
        static fn (Request $request): bool => $request->method() === 'DELETE',
    );
});
