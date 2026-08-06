<?php

declare(strict_types=1);

return [

    /*
    |--------------------------------------------------------------------------
    | Default Spoolrail Connection
    |--------------------------------------------------------------------------
    |
    | Spoolrail supports multiple message transports through a single, unified
    | interface. The default connection below is used unless another connection
    | is explicitly selected when publishing or consuming messages.
    |
    */

    'default' => env('SPOOLRAIL_CONNECTION', 'rabbitmq'),

    /*
    |--------------------------------------------------------------------------
    | Receive-Side Ownership Prefix
    |--------------------------------------------------------------------------
    |
    | This prefix namespaces receive-side resources owned by this application.
    | Before consuming or managing subscriptions, choose up to 24 characters
    | from the application's durable identity. It is recommended to keep this
    | value independent of APP_NAME because changing it requires migrating
    | subscriptions.
    |
    */

    'prefix' => env('SPOOLRAIL_PREFIX'),

    /*
    |--------------------------------------------------------------------------
    | Spoolrail Connections
    |--------------------------------------------------------------------------
    |
    | Here you may configure every Spoolrail connection used by your application.
    | The in-process "array" driver is intended for tests and local simulation.
    |
    | Supported drivers: "array", "rabbitmq"
    |
    */

    'connections' => [

        'array' => [
            'driver' => 'array',
        ],

        'rabbitmq' => [
            'driver' => 'rabbitmq',
            'scheme' => env('RABBITMQ_SCHEME', 'amqp'),
            'host' => env('RABBITMQ_HOST', '127.0.0.1'),
            'port' => env('RABBITMQ_PORT', 5672),
            'username' => env('RABBITMQ_USERNAME', 'guest'),
            'password' => env('RABBITMQ_PASSWORD', 'guest'),
            'vhost' => env('RABBITMQ_VHOST', '/'),
            'ca_file' => null,
            'connection_timeout' => 3,
            'publisher_confirm_timeout' => 60,
            'heartbeat' => 60,
            'prefetch' => 10,
            'management' => [
                'url' => env('RABBITMQ_MANAGEMENT_URL', 'http://127.0.0.1:15672'),
                'username' => env('RABBITMQ_MANAGEMENT_USERNAME', env('RABBITMQ_USERNAME', 'guest')),
                'password' => env('RABBITMQ_MANAGEMENT_PASSWORD', env('RABBITMQ_PASSWORD', 'guest')),
                'ca_file' => null,
            ],
        ],

    ],

    /*
    |--------------------------------------------------------------------------
    | Transactional Outbox
    |--------------------------------------------------------------------------
    |
    | Enable the outbox when publications must commit atomically with database
    | changes. The connection defaults to Laravel's default database connection.
    | Failed publication reports for the same row are throttled for the number
    | of seconds configured below.
    |
    */

    'outbox' => [
        'enabled' => env('SPOOLRAIL_OUTBOX', false),
        'connection' => env('SPOOLRAIL_OUTBOX_CONNECTION', null),
        'exception_cooldown' => 300,
    ],

    /*
    |--------------------------------------------------------------------------
    | Message Deduplication
    |--------------------------------------------------------------------------
    |
    | Spoolrail hands each consumed message to Laravel Queue at least once,
    | so a failure between the Queue handoff and the broker acknowledgement
    | may queue the same message again. Spoolrail remembers handled messages
    | in the cache store below for "remember" seconds and skips a remembered
    | duplicate instead of handling it again. One handling attempt may hold
    | the per-message lock for "lock" seconds, which should exceed the
    | slowest handler run.
    |
    */

    'deduplication' => [
        'enabled' => env('SPOOLRAIL_DEDUPLICATION', true),
        'store' => env('SPOOLRAIL_DEDUPLICATION_STORE', env('CACHE_STORE', 'database')),
        'remember' => 86400,
        'lock' => 300,
    ],

];
