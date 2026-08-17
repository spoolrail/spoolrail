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
    | Supported drivers: "array", "rabbitmq", "snssqs", "pubsub"
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

        'snssqs' => [
            'driver' => 'snssqs',
            'key' => env('AWS_ACCESS_KEY_ID'),
            'secret' => env('AWS_SECRET_ACCESS_KEY'),
            'token' => env('AWS_SESSION_TOKEN'),
            'region' => env('AWS_DEFAULT_REGION', 'us-east-1'),
            'account_id' => env('AWS_ACCOUNT_ID'),
            'endpoint' => env('SPOOLRAIL_AWS_ENDPOINT'),
            'fifo' => true,
            'connection_timeout' => 3,
            'request_timeout' => 60,
        ],

        'pubsub' => [
            'driver' => 'pubsub',
            'project_id' => env('GOOGLE_CLOUD_PROJECT'),
            'credentials' => env('SPOOLRAIL_GOOGLE_CREDENTIALS'),
            'endpoint' => env('SPOOLRAIL_GOOGLE_PUBSUB_ENDPOINT'),
            'message_ordering' => true,
            'exactly_once' => true,
        ],

    ],

    /*
    |--------------------------------------------------------------------------
    | Publisher Retries
    |--------------------------------------------------------------------------
    |
    | Broker publication failures are retried this many times after the first
    | attempt unless permanently rejected, with a fixed wait before each retry.
    |
    */

    'publisher_retries' => [
        'times' => 2,
        'delay_milliseconds' => 1000,
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
    | Concurrency allows different topics to publish at the same time. Publications
    | for each topic remain ordered within its broker connection. The default is one.
    | Failed publication reports for the same row are throttled for the number
    | of seconds configured below.
    |
    */

    'outbox' => [
        'enabled' => env('SPOOLRAIL_OUTBOX', false),
        'database_connection' => env('SPOOLRAIL_OUTBOX_DATABASE_CONNECTION', env('DB_CONNECTION', 'sqlite')),
        'concurrency' => env('SPOOLRAIL_OUTBOX_CONCURRENCY', 1),
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
