<?php

declare(strict_types=1);

use Illuminate\Support\Str;

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
    | Set it explicitly when resource names must remain stable across application
    | or environment renames.
    |
    */

    'prefix' => env(
        'SPOOLRAIL_PREFIX',
        Str::slug((string) env('APP_NAME', 'laravel')).'-'.Str::slug((string) env('APP_ENV', 'local')),
    ),

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

];
