<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Topology;

use Spoolrail\Spoolrail\Exceptions\InvalidTopicException;
use Spoolrail\Spoolrail\Exceptions\ManagedTopologyUnavailableException;
use Spoolrail\Spoolrail\LogicalName;
use Spoolrail\Spoolrail\SpoolrailManager;

readonly class DeleteTopic
{
    public function __construct(
        private SpoolrailManager $manager,
    ) {}

    public function __invoke(string $connectionName, string $topic): void
    {
        if (! LogicalName::isValid($topic)) {
            throw new InvalidTopicException($topic);
        }

        $topology = $this->manager->connection($connectionName)->managedTopology()
            ?? throw new ManagedTopologyUnavailableException($connectionName);

        $topology->deleteTopic($topic);
    }
}
