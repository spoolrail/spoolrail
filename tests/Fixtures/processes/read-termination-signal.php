<?php

declare(strict_types=1);

use Illuminate\Cache\FileStore;
use Illuminate\Cache\Repository as CacheRepository;
use Illuminate\Config\Repository as ConfigRepository;
use Illuminate\Filesystem\Filesystem;
use Spoolrail\Spoolrail\Subscriptions\TerminationSignal;

require dirname(__DIR__, 3).'/vendor/autoload.php';

$directory = $argv[1] ?? '';
$hostname = $argv[2] ?? '';
$signal = new TerminationSignal(
    new CacheRepository(new FileStore(new Filesystem, $directory)),
    new ConfigRepository(['cache' => ['default' => 'file']]),
    $hostname,
);

fwrite(STDOUT, $signal->current() ?? '');
