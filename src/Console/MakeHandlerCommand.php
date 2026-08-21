<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Console;

use Illuminate\Console\GeneratorCommand;

class MakeHandlerCommand extends GeneratorCommand
{
    protected $signature = 'make:handler
                            {name : The name of the message handler}
                            {--f|force : Create the class even if the message handler already exists}';

    protected $description = 'Create a new Spoolrail message handler class';

    protected $type = 'Message handler';

    protected function getStub(): string
    {
        return __DIR__.'/stubs/message-handler.stub';
    }

    protected function getDefaultNamespace($rootNamespace): string
    {
        return $rootNamespace.'\Messages';
    }
}
