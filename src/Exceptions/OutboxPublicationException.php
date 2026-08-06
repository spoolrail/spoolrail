<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use RuntimeException;
use Spoolrail\Spoolrail\Enums\PublicationOutcome;
use Spoolrail\Spoolrail\Message;

class OutboxPublicationException extends RuntimeException implements SpoolrailException
{
    public readonly PublicationOutcome $outcome;

    public function __construct(
        public readonly int $outboxId,
        public readonly Message $logicalMessage,
        public readonly string $connectionName,
        public readonly string $topic,
        PublicationException $previous,
    ) {
        $this->outcome = $previous->outcome;

        parent::__construct(
            "Spoolrail could not publish outbox row [$outboxId] for message [$logicalMessage->id].",
            previous: $previous,
        );
    }

    /**
     * @return array<string, int|string>
     */
    public function context(): array
    {
        return [
            'spoolrail_outbox_id' => $this->outboxId,
            'spoolrail_message_id' => $this->logicalMessage->id,
            'spoolrail_connection' => $this->connectionName,
            'spoolrail_topic' => $this->topic,
            'spoolrail_publication_outcome' => $this->outcome->name,
        ];
    }
}
