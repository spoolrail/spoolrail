<?php

declare(strict_types=1);

use Illuminate\Filesystem\Filesystem;
use Spoolrail\Spoolrail\SpoolrailServiceProvider;

$files = new Filesystem;
$applicationPath = '';
$originalBasePath = '';

beforeEach(function () use ($files, &$applicationPath, &$originalBasePath): void {
    $originalBasePath = app()->basePath();
    $applicationPath = sys_get_temp_dir().'/spoolrail-install-'.bin2hex(random_bytes(8));

    $files->ensureDirectoryExists($applicationPath);
    app()->setBasePath($applicationPath);

    new SpoolrailServiceProvider(app())->boot();
});

afterEach(function () use ($files, &$applicationPath, &$originalBasePath): void {
    app()->setBasePath($originalBasePath);
    new SpoolrailServiceProvider(app())->boot();

    $files->deleteDirectory($applicationPath);
});

test('installs the configuration and subscription routes without the optional migration', function () use ($files, &$applicationPath): void {
    $exitCode = $this->artisan('spoolrail:install')->run();

    expect($exitCode)->toBe(0);
    expect($files->get("$applicationPath/config/spoolrail.php"))->toBe(
        $files->get(dirname(__DIR__, 3).'/config/spoolrail.php'),
    );
    expect($files->get("$applicationPath/routes/subscriptions.php"))->toBe(<<<'PHP'
<?php

use Spoolrail\Spoolrail\Facades\Spoolrail;

/*
Spoolrail::subscribe(
    topic: 'topic-name',
    name: 'subscription-name',
    handler: \App\Messages\ExampleHandler::class,
);
*/

PHP);
    expect($files->glob("$applicationPath/database/migrations/*_create_outbox_publications_table.php"))->toBe([]);
});

test('publishes the outbox migration without replacing an existing installation', function () use ($files, &$applicationPath): void {
    // --- Arrange ---
    $this->artisan('spoolrail:install')->run();
    $files->put("$applicationPath/config/spoolrail.php", "<?php\n\nreturn ['custom' => true];\n");
    $files->put("$applicationPath/routes/subscriptions.php", "<?php\n\n// Application subscriptions.\n");

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:install', ['--migrations' => true])->run();

    // --- Assert ---
    $migrations = $files->glob("$applicationPath/database/migrations/*_create_outbox_publications_table.php");

    expect($exitCode)->toBe(0);
    expect($files->get("$applicationPath/config/spoolrail.php"))->toBe("<?php\n\nreturn ['custom' => true];\n");
    expect($files->get("$applicationPath/routes/subscriptions.php"))->toBe("<?php\n\n// Application subscriptions.\n");
    expect($migrations)->toHaveCount(1);
    expect($files->get($migrations[0]))->toBe(
        $files->get(dirname(__DIR__, 3).'/database/migrations/0001_01_01_000000_create_outbox_publications_table.php'),
    );
});

test('preserves an existing outbox migration on later runs', function () use ($files, &$applicationPath): void {
    // --- Arrange ---
    $this->artisan('spoolrail:install', ['--migrations' => true])->run();
    $migration = $files->glob("$applicationPath/database/migrations/*_create_outbox_publications_table.php")[0];
    $files->put($migration, "<?php\n\n// Application migration.\n");

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:install', ['--migrations' => true])->run();

    // --- Assert ---
    expect($exitCode)->toBe(0);
    expect($files->glob("$applicationPath/database/migrations/*_create_outbox_publications_table.php"))->toBe([$migration]);
    expect($files->get($migration))->toBe("<?php\n\n// Application migration.\n");
});

test('does not publish the optional migration when only forced', function () use ($files, &$applicationPath): void {
    $exitCode = $this->artisan('spoolrail:install', ['--force' => true])->run();

    expect($exitCode)->toBe(0);
    expect($files->glob("$applicationPath/database/migrations/*_create_outbox_publications_table.php"))->toBe([]);
});

test('replaces every requested file when forced', function () use ($files, &$applicationPath): void {
    // --- Arrange ---
    $this->artisan('spoolrail:install', ['--migrations' => true])->run();
    $migration = $files->glob("$applicationPath/database/migrations/*_create_outbox_publications_table.php")[0];

    $files->put("$applicationPath/config/spoolrail.php", "<?php\n\nreturn ['custom' => true];\n");
    $files->put("$applicationPath/routes/subscriptions.php", "<?php\n\n// Application subscriptions.\n");
    $files->put($migration, "<?php\n\n// Application migration.\n");

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:install', [
        '--force' => true,
        '--migrations' => true,
    ])->run();

    // --- Assert ---
    $migrations = $files->glob("$applicationPath/database/migrations/*_create_outbox_publications_table.php");

    expect($exitCode)->toBe(0);
    expect($files->get("$applicationPath/config/spoolrail.php"))->toBe(
        $files->get(dirname(__DIR__, 3).'/config/spoolrail.php'),
    );
    expect($files->get("$applicationPath/routes/subscriptions.php"))->toBe(
        $files->get(dirname(__DIR__, 3).'/src/Console/stubs/subscription-routes.stub'),
    );
    expect($migrations)->toBe([$migration]);
    expect($files->get($migration))->toBe(
        $files->get(dirname(__DIR__, 3).'/database/migrations/0001_01_01_000000_create_outbox_publications_table.php'),
    );
});

test('fails without changing migrations when multiple outbox migrations exist', function () use ($files, &$applicationPath): void {
    // --- Arrange ---
    $this->artisan('spoolrail:install', ['--migrations' => true])->run();
    $migration = $files->glob("$applicationPath/database/migrations/*_create_outbox_publications_table.php")[0];
    $duplicate = "$applicationPath/database/migrations/2099_01_01_000000_create_outbox_publications_table.php";

    $files->put($migration, "<?php\n\n// First migration.\n");
    $files->put($duplicate, "<?php\n\n// Duplicate migration.\n");

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:install', [
        '--force' => true,
        '--migrations' => true,
    ])->run();

    // --- Assert ---
    expect($exitCode)->toBe(1);
    expect($files->get($migration))->toBe("<?php\n\n// First migration.\n");
    expect($files->get($duplicate))->toBe("<?php\n\n// Duplicate migration.\n");
});
