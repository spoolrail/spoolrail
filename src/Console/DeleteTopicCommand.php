<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\Command;
use Spoolrail\Spoolrail\SpoolrailManager;
use Spoolrail\Spoolrail\Topology\DeleteTopic;

class DeleteTopicCommand extends Command
{
    protected $signature = 'spoolrail:delete-topic
                            {topic : Logical topic to delete}
                            {--connection= : Spoolrail connection containing the topic}';

    protected $description = 'Delete one unused logical topic without deleting subscriptions';

    public function handle(DeleteTopic $deleteTopic, SpoolrailManager $manager): int
    {
        $connectionOption = $this->option('connection');

        if ($connectionOption !== null && trim($connectionOption) === '') {
            $this->components->error('The --connection option must name a Spoolrail connection.');

            return self::FAILURE;
        }

        $connectionName = $connectionOption ?? $manager->defaultConnectionName();
        $topic = $this->argument('topic');

        $deleteTopic($connectionName, $topic);
        $this->components->info("Deleted topic [$topic] from connection [$connectionName].");

        return self::SUCCESS;
    }
}
