<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Schema;

test('indexes outbox publications by dispatch lane and sequence', function (): void {
    // --- Act ---
    $migration = require dirname(__DIR__, 3).'/database/migrations/0001_01_01_000000_create_outbox_publications_table.php';

    $migration->up();

    // --- Assert ---
    $laneIndex = collect(Schema::getIndexes('outbox_publications'))
        ->first(fn (array $index): bool => $index['columns'] === ['connection', 'topic', 'id']);

    expect($laneIndex)->not->toBeNull();
});
