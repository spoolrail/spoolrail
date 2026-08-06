<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Schema;
use Spoolrail\Spoolrail\Tests\Concerns\InteractsWithOutbox;

uses(InteractsWithOutbox::class);

test('creates the published outbox storage contract', function (): void {
    // --- Act ---
    $this->migrateOutbox();

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
