<?php

declare(strict_types=1);

use Illuminate\Config\Repository;
use Illuminate\Foundation\Application;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use Spoolrail\Spoolrail\MessageSerializer;
use Spoolrail\Spoolrail\SpoolrailManager;

$loader = require dirname(__DIR__, 2).'/vendor/autoload.php';

$loader->unregister();
spl_autoload_register(static function (string $class) use ($loader): void {
    if (! str_starts_with($class, 'PhpAmqpLib\\')) {
        $loader->loadClass($class);
    }
});

$application = new Application;
$configuration = new Repository([
    'spoolrail' => [
        'default' => 'rabbit',
        'connections' => [
            'rabbit' => [
                'driver' => 'rabbitmq',
                'url' => 'amqp://guest:guest@127.0.0.1:1/%2F',
            ],
        ],
    ],
]);
$serializer = new MessageSerializer;

if (class_exists(AMQPConnectionConfig::class, false)) {
    throw new LogicException('php-amqplib was loaded before dependency isolation.');
}

try {
    new SpoolrailManager($application, $configuration, $serializer)->connection('rabbit');
} catch (Throwable $exception) {
    echo json_encode([
        'class' => $exception::class,
        'message' => $exception->getMessage(),
    ], JSON_THROW_ON_ERROR);

    exit(0);
}

echo json_encode([
    'class' => null,
    'message' => null,
], JSON_THROW_ON_ERROR);
