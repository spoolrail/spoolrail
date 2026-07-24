<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use InvalidArgumentException;

class InvalidTopicException extends InvalidArgumentException implements SpoolrailException
{
    public function __construct(string $topic)
    {
        parent::__construct(
            "Topic [$topic] must contain at least three ASCII characters, begin with a letter, and otherwise contain only letters, digits, hyphens, and underscores.",
        );
    }
}
