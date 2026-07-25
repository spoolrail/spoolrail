<?php

declare(strict_types=1);

use Illuminate\Support\Str;

return [

    /*
    |--------------------------------------------------------------------------
    | Default Spoolrail Connection
    |--------------------------------------------------------------------------
    |
    | This connection is used whenever none is selected explicitly. Its name
    | must match one of the connections defined below.
    |
    */

    'default' => env('SPOOLRAIL_CONNECTION', 'array'),

    /*
    |--------------------------------------------------------------------------
    | Receive-Side Ownership Prefix
    |--------------------------------------------------------------------------
    |
    | Set an explicit prefix to keep the resource namespace stable across
    | application or environment renames.
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
    | Each connection must declare a driver. Additional options are passed to
    | custom driver factories unchanged.
    |
    */

    'connections' => [

        'array' => [
            'driver' => 'array',
        ],

        'rabbitmq' => [
            'driver' => 'rabbitmq',
            'scheme' => env('RABBITMQ_SCHEME', 'amqp'),

            // Replace "host" with a "hosts" array of hostnames for a multi-host connection.
            'host' => env('RABBITMQ_HOST', '127.0.0.1'),

            'port' => env('RABBITMQ_PORT', 5672),
            'username' => env('RABBITMQ_USERNAME', 'guest'),
            'password' => env('RABBITMQ_PASSWORD', 'guest'),
            'vhost' => env('RABBITMQ_VHOST', '/'),
            'ca_file' => null,
            'connection_timeout' => 3,
            'heartbeat' => 60,
            'publisher_confirm_timeout' => 60,
            'consumer_ack_timeout' => null,
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
