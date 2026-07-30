<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use InvalidArgumentException;
use LogicException;
use Spoolrail\Spoolrail\SpoolrailManager;

readonly class DeleteTopic
{
    public function __construct(
        private SpoolrailManager $manager,
    ) {}

    public function __invoke(string $connectionName, string $topic): void
    {
        if (! LogicalName::isValid($topic)) {
            throw new InvalidArgumentException(
                "Topic [$topic] must contain at least three ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.",
            );
        }

        $topology = $this->manager->connection($connectionName)->managedTopology()
            ?? throw new LogicException("Spoolrail connection [$connectionName] does not provide package-managed topology.");

        $topology->deleteTopic($topic);
    }
}
