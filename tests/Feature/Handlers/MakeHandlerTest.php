<?php

declare(strict_types=1);

use Illuminate\Filesystem\Filesystem;

$files = new Filesystem;
$applicationPath = '';
$originalBasePath = '';

beforeEach(function () use ($files, &$applicationPath, &$originalBasePath): void {
    $originalBasePath = app()->basePath();
    $applicationPath = sys_get_temp_dir().'/spoolrail-handler-'.bin2hex(random_bytes(8));

    $files->ensureDirectoryExists("$applicationPath/app");
    $files->put("$applicationPath/composer.json", <<<'JSON'
{
    "autoload": {
        "psr-4": {
            "Warehouse\\": "app/"
        }
    }
}
JSON);

    app()->setBasePath($applicationPath);
});

afterEach(function () use ($files, &$applicationPath, &$originalBasePath): void {
    app()->setBasePath($originalBasePath);
    $files->deleteDirectory($applicationPath);
});

test('creates a message handler in the application messages namespace', function () use ($files, &$applicationPath): void {
    $exitCode = $this->artisan('make:handler', ['name' => 'ReserveInventoryHandler'])->run();

    expect($exitCode)->toBe(0);
    expect($files->get("$applicationPath/app/Messages/ReserveInventoryHandler.php"))->toBe(<<<'PHP'
<?php

namespace Warehouse\Messages;

use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Message;

class ReserveInventoryHandler implements MessageHandler
{
    public function handle(Message $message): void
    {
        //
    }
}

PHP);
});
