<?php

declare(strict_types=1);

use Illuminate\Http\Client\Factory;
use Illuminate\Http\Client\Request;
use Spoolrail\Spoolrail\Exceptions\RabbitMqManagementException;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqConnectionConfig;
use Spoolrail\Spoolrail\RabbitMq\RabbitMqManagementClient;

test('maps the Management HTTPS endpoint identity and private CA into a verified HTTP request', function (): void {
    // --- Arrange ---
    $http = new Factory;
    $http->fake([
        '*' => $http->response(['rabbitmq_version' => '4.3.2']),
    ]);
    $client = new RabbitMqManagementClient(
        new RabbitMqConnectionConfig('events', [
            'username' => 'publisher',
            'password' => 'runtime-secret',
            'management' => [
                'url' => 'https://management.internal:15671',
                'username' => 'topology',
                'password' => 'control-secret',
                'ca_file' => __FILE__,
            ],
        ]),
        $http,
    );
    $sent = null;
    $options = null;

    // --- Act ---
    $client->pendingRequest()
        ->beforeSending(function (Request $request, array $requestOptions) use (&$sent, &$options): void {
            $sent = $request;
            $options = $requestOptions;
        })
        ->get('overview');

    // --- Assert ---
    expect($sent)->toBeInstanceOf(Request::class);
    expect($sent?->url())->toBe('https://management.internal:15671/api/overview');
    expect($sent?->header('Authorization'))->toBe([
        'Basic '.base64_encode('topology:control-secret'),
    ]);
    expect($options['verify'] ?? null)->toBe(__FILE__);
});

test('uses the system trust store and AMQP credentials when management overrides are absent', function (): void {
    // --- Arrange ---
    $http = new Factory;
    $http->fake([
        '*' => $http->response(['rabbitmq_version' => '4.3.2']),
    ]);
    $client = new RabbitMqManagementClient(
        new RabbitMqConnectionConfig('events', [
            'username' => 'publisher',
            'password' => 'runtime-secret',
            'ca_file' => __FILE__,
            'management' => [
                'url' => 'https://management.internal:15671/api',
            ],
        ]),
        $http,
    );
    $sent = null;
    $options = null;

    // --- Act ---
    $client->pendingRequest()
        ->beforeSending(function (Request $request, array $requestOptions) use (&$sent, &$options): void {
            $sent = $request;
            $options = $requestOptions;
        })
        ->get('overview');

    // --- Assert ---
    expect($sent?->header('Authorization'))->toBe([
        'Basic '.base64_encode('publisher:runtime-secret'),
    ]);
    expect($options['verify'] ?? null)->toBeTrue();
});

test('filters owned queues by a literal prefix with bounded low-cost pagination', function (): void {
    // --- Arrange ---
    $http = new Factory;
    $http->fake([
        '*' => $http->response([
            'page' => 1,
            'page_count' => 0,
            'items' => [],
        ]),
    ]);
    $client = new RabbitMqManagementClient(
        new RabbitMqConnectionConfig('events', []),
        $http,
    );

    // --- Act ---
    $queues = $client->queuesOwnedBy('warehouse-production');

    // --- Assert ---
    expect($queues)->toBe([]);
    $http->assertSent(function (Request $request): bool {
        parse_str(parse_url($request->url(), PHP_URL_QUERY) ?: '', $query);

        return $request->method() === 'GET'
            && parse_url($request->url(), PHP_URL_PATH) === '/api/queues/%2F'
            && $query === [
                'name' => '^warehouse\\-production\\-',
                'use_regex' => 'true',
                'pagination' => 'true',
                'page' => '1',
                'page_size' => '500',
                'disable_stats' => 'true',
            ];
    });
});

test('returns owned queues from every Management API page', function (): void {
    // --- Arrange ---
    $http = new Factory;
    $firstPage = [['name' => 'warehouse-production-first']];
    $secondPage = [['name' => 'warehouse-production-second']];
    $http->fake([
        '*' => $http->sequence()
            ->push([
                'page' => 1,
                'page_count' => 2,
                'items' => $firstPage,
            ])
            ->push([
                'page' => 2,
                'page_count' => 2,
                'items' => $secondPage,
            ]),
    ]);
    $client = new RabbitMqManagementClient(
        new RabbitMqConnectionConfig('events', []),
        $http,
    );

    // --- Act ---
    $queues = $client->queuesOwnedBy('warehouse-production');

    // --- Assert ---
    expect($queues)->toBe([...$firstPage, ...$secondPage]);
    expect(
        $http->recorded()->map(function (array $pair): string {
            parse_str(parse_url($pair[0]->url(), PHP_URL_QUERY) ?: '', $query);

            return $query['page'];
        })->all(),
    )->toBe(['1', '2']);
});

test('rejects an invalid paginated queue response', function (): void {
    $http = new Factory;
    $http->fake([
        '*' => $http->response([
            'page' => 1,
            'items' => [],
        ]),
    ]);
    $client = new RabbitMqManagementClient(
        new RabbitMqConnectionConfig('events', []),
        $http,
    );

    expect(fn (): array => $client->queuesOwnedBy('warehouse-production'))
        ->toThrow(
            RabbitMqManagementException::class,
            'RabbitMQ connection [events] Management API returned an invalid response while listing queues owned by prefix [warehouse-production].',
        );
});

test('reports Management API authentication and permission failures without credentials', function (int $status): void {
    $http = new Factory;
    $http->fake([
        '*' => $http->response(status: $status),
    ]);
    $client = new RabbitMqManagementClient(
        new RabbitMqConnectionConfig('events', [
            'host' => 'rabbit.internal',
            'username' => 'publisher',
            'password' => 'runtime-secret',
            'management' => [
                'url' => 'https://management.internal:15671',
                'username' => 'topology',
                'password' => 'control-secret',
            ],
        ]),
        $http,
    );

    expect(fn (): array => $client->overview())
        ->toThrow(function (RabbitMqManagementException $exception) use ($status): void {
            expect($exception->getMessage())
                ->toBe("RabbitMQ connection [events] Management API returned HTTP $status while reading the broker version.")
                ->not->toContain('runtime-secret')
                ->not->toContain('control-secret');
        });
})->with([
    'authentication failure' => 401,
    'permission failure' => 403,
]);

test('preserves TLS request failure diagnostics without surfacing credentials', function (): void {
    $http = new Factory;
    $http->fake([
        '*' => Factory::failedConnection(
            'cURL error 60: SSL certificate problem: unable to get local issuer certificate',
        ),
    ]);
    $client = new RabbitMqManagementClient(
        new RabbitMqConnectionConfig('events', [
            'username' => 'publisher',
            'password' => 'runtime-secret',
            'management' => [
                'url' => 'https://management.internal:15671',
                'username' => 'topology',
                'password' => 'control-secret',
            ],
        ]),
        $http,
    );

    expect(fn (): array => $client->overview())
        ->toThrow(function (RabbitMqManagementException $exception): void {
            expect($exception->getMessage())->toBe(
                'RabbitMQ connection [events] Management API request failed while reading the broker version.',
            );
            expect($exception->getPrevious()?->getMessage())->toContain('SSL certificate problem');

            $diagnostics = $exception->getMessage().' '.($exception->getPrevious()?->getMessage() ?? '');

            expect($diagnostics)
                ->not->toContain('runtime-secret')
                ->not->toContain('control-secret');
        });
});
