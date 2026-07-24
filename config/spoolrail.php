<?php

declare(strict_types=1);

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
    | Null derives the prefix from the application name and environment. An
    | explicit prefix is validated as supplied and is never normalized.
    |
    */

    'prefix' => env('SPOOLRAIL_PREFIX'),

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

    ],

];
