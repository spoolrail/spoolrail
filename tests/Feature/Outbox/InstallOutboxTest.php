<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Schema;

test('creates the published outbox storage contract', function (): void {
    // --- Act ---
    $migration = require dirname(__DIR__, 3).'/database/migrations/0001_01_01_000000_create_outbox_publications_table.php';

    $migration->up();

    // --- Assert ---
    expect(Schema::getColumnListing('outbox_publications'))->toBe([
        'id',
        'connection',
        'topic',
        'message',
        'headers',
        'last_error',
        'created_at',
        'updated_at',
    ]);

    $laneIndex = collect(Schema::getIndexes('outbox_publications'))
        ->first(fn (array $index): bool => $index['columns'] === ['connection', 'topic', 'id']);

    expect($laneIndex)->not->toBeNull();
});
