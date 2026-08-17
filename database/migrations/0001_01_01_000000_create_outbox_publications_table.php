<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function getConnection(): ?string
    {
        $connection = config('spoolrail.outbox.database_connection');

        return is_string($connection) ? $connection : null;
    }

    public function up(): void
    {
        Schema::connection($this->getConnection())->create('outbox_publications', function (Blueprint $table): void {
            $table->id();
            $table->string('connection');
            $table->string('topic', 251);
            $table->json('message');
            $table->json('headers');
            $table->string('ordering_key', 128)->nullable();
            $table->string('last_error', 500)->nullable();
            $table->timestamps();

            $table->index(['connection', 'topic', 'id']);
        });
    }
};
