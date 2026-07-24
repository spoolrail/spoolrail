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

    public function handle(DeleteTopic $deletion, SpoolrailManager $manager): int
    {
        $connectionOption = $this->option('connection');

        if ($connectionOption !== null && trim($connectionOption) === '') {
            $this->components->error('The --connection option must name a Spoolrail connection.');

            return self::FAILURE;
        }

        $connection = $connectionOption ?? $manager->getDefaultConnection();
        $topic = $this->argument('topic');

        $deletion->run($connection, $topic);
        $this->components->info("Deleted topic [$topic] from connection [$connection].");

        return self::SUCCESS;
    }
}
