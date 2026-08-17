<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Outbox;

use Illuminate\Database\Eloquent\Model;
use Override;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;

/**
 * @property int $id
 * @property string $connection
 * @property string $topic
 * @property array{
 *     id: string,
 *     type: string,
 *     payload: array<array-key, mixed>,
 *     published_at: string
 * } $message
 * @property array<string, string> $headers
 * @property string|null $ordering_key
 * @property string|null $last_error
 * @property int $head_id
 * @property int $backlog_size
 */
class OutboxPublication extends Model
{
    protected $guarded = [];

    #[Override]
    public function getConnectionName(): ?string
    {
        $connection = parent::getConnectionName();

        if ($connection !== null) {
            return $connection;
        }

        $connection = config('spoolrail.outbox.database_connection');

        if ($connection === null) {
            return null;
        }

        if (! is_string($connection) || trim($connection) === '') {
            throw InvalidConfigException::invalidOutboxConnection();
        }

        return $connection;
    }

    /**
     * @return array<string, string>
     */
    #[Override]
    protected function casts(): array
    {
        return [
            'message' => 'json',
            'headers' => 'json',
            'head_id' => 'integer',
            'backlog_size' => 'integer',
        ];
    }

    public static function highestId(): ?int
    {
        return static::query()
            ->orderByDesc('id')
            ->first(['id'])
            ?->id;
    }

    /**
     * @return list<OutboxLane>
     */
    public static function lanesThrough(int $highestPublicationId): array
    {
        $rows = static::query()
            ->selectRaw('MIN(id) AS head_id')
            ->selectRaw('COUNT(*) AS backlog_size')
            ->where('id', '<=', $highestPublicationId)
            ->groupBy('connection', 'topic')
            ->get();

        $lanes = [];

        foreach ($rows as $row) {
            $lanes[] = new OutboxLane(
                headId: $row->head_id,
                backlogSize: $row->backlog_size,
            );
        }

        return $lanes;
    }

    public static function nextHeadInLane(
        string $connectionName,
        string $topic,
        int $highestPublicationId,
    ): ?self {
        return static::query()
            ->select(['id', 'last_error'])
            ->where('connection', $connectionName)
            ->where('topic', $topic)
            ->where('id', '<=', $highestPublicationId)
            ->orderBy('id')
            ->first();
    }

    public function recordFailure(string $summary): void
    {
        $this->forceFill(['last_error' => $summary])->touch();
    }
}
