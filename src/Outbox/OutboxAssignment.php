<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Outbox;

use JsonException;
use UnexpectedValueException;

readonly class OutboxAssignment
{
    /**
     * @param  non-empty-list<int>  $laneHeadIds
     */
    public function __construct(
        public int $highestPublicationId,
        public array $laneHeadIds,
    ) {}

    /**
     * @param  non-empty-list<OutboxLane>  $lanes
     */
    public static function fromLanes(int $highestPublicationId, array $lanes): self
    {
        return new self(
            $highestPublicationId,
            array_map(
                static fn (OutboxLane $lane): int => $lane->headId,
                $lanes,
            ),
        );
    }

    /**
     * @throws JsonException
     */
    public function toJson(): string
    {
        return json_encode([
            'highest_publication_id' => $this->highestPublicationId,
            'lane_head_ids' => $this->laneHeadIds,
        ], JSON_THROW_ON_ERROR);
    }

    /**
     * @throws JsonException
     */
    public static function fromJson(string $json): self
    {
        $value = json_decode($json, true, flags: JSON_THROW_ON_ERROR);

        if (! is_array($value)) {
            throw new UnexpectedValueException('Invalid Spoolrail outbox worker assignment.');
        }

        return new self(
            self::positiveInteger($value['highest_publication_id'] ?? null),
            self::positiveIntegers($value['lane_head_ids'] ?? null),
        );
    }

    private static function positiveInteger(mixed $value): int
    {
        if (! is_int($value) || $value < 1) {
            throw new UnexpectedValueException('Invalid Spoolrail outbox worker assignment.');
        }

        return $value;
    }

    /**
     * @return non-empty-list<int>
     */
    private static function positiveIntegers(mixed $value): array
    {
        if (! is_array($value)) {
            throw new UnexpectedValueException('Invalid Spoolrail outbox worker assignment.');
        }

        if ($value === [] || ! array_is_list($value)) {
            throw new UnexpectedValueException('Invalid Spoolrail outbox worker assignment.');
        }

        return array_map(
            self::positiveInteger(...),
            $value,
        );
    }
}
