<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Tests\Concerns;

trait InteractsWithOutbox
{
    protected function setUpInteractsWithOutbox(): void
    {
        config()->set('spoolrail.outbox.enabled', true);

        $this->migrateOutbox();
    }

    protected function migrateOutbox(): void
    {
        $migration = require dirname(__DIR__, 2).'/database/migrations/0001_01_01_000000_create_outbox_publications_table.php';

        $migration->up();
    }
}
