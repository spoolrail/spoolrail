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
    | Consumer Supervision
    |--------------------------------------------------------------------------
    |
    | Repeated reports for the same consumer failure category are throttled
    | for the number of seconds configured below.
    |
    */

    'consumer' => [
        'exception_cooldown' => 300,
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
        'connection' => env('SPOOLRAIL_OUTBOX_CONNECTION', env('DB_CONNECTION', 'sqlite')),
        'exception_cooldown' => 300,
    ],

    /*
    |--------------------------------------------------------------------------
    | Queue Handoff Idempotency
    |--------------------------------------------------------------------------
    |
    | If Spoolrail completes its Laravel Queue handoff but the broker
    | acknowledgement is lost, the broker may retry the delivery. Spoolrail
    | recognizes the recent handoff and does not add another job.
    |
    | It is recommended to use a "database" or "redis" store in production
    | because they provide atomic lock release and automatically clean up
    | expired locks. The default expiry covers ordinary recovery while
    | keeping lock storage small, and should not normally be changed.
    |
    */

    'handoff_idempotency' => [
        'cache_store' => env('SPOOLRAIL_HANDOFF_IDEMPOTENCY_CACHE_STORE', env('CACHE_STORE', 'database')),
        'expiry' => 600,
    ],

];
