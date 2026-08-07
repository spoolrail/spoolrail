<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use InvalidArgumentException;
use LogicException;
use Spoolrail\Spoolrail\SpoolrailManager;

class DeleteTopic
{
    public function __construct(
        private SpoolrailManager $manager,
    ) {}

    public function __invoke(string $connectionName, string $topic): void
    {
        if (! LogicalName::isValidTopic($topic)) {
            throw new InvalidArgumentException(
                "Topic [$topic] must contain between 3 and 251 ASCII characters, begin with a letter, otherwise contain only letters, digits, hyphens, and underscores, and avoid transport-reserved beginnings.",
            );
        }

        $topology = $this->manager->connection($connectionName)->topology()
            ?? throw new LogicException("Spoolrail connection [$connectionName] does not provide package-managed topology.");

        $topology->deleteTopic($topic);
    }
}
