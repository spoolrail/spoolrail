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
 * @property string|null $last_error
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

        $connection = config('spoolrail.outbox.connection');

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
     * @return list<self>
     */
    public static function headsThrough(int $highestId): array
    {
        $headIds = static::query()
            ->toBase()
            ->selectRaw('MIN(id)')
            ->where('id', '<=', $highestId)
            ->groupBy('connection', 'topic');

        $heads = static::query()
            ->select(['id', 'last_error'])
            ->whereIn('id', $headIds)
            ->get()
            ->all();

        return array_values($heads);
    }

    public static function nextHeadInLane(
        string $connectionName,
        string $topic,
        int $highestId,
    ): ?self {
        return static::query()
            ->select(['id', 'last_error'])
            ->where('connection', $connectionName)
            ->where('topic', $topic)
            ->where('id', '<=', $highestId)
            ->orderBy('id')
            ->first();
    }

    public function recordFailure(string $summary): void
    {
        $this->forceFill(['last_error' => $summary])->touch();
    }
}
