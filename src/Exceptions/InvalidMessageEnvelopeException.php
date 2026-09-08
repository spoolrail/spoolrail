<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Exceptions;

use JsonException;
use UnexpectedValueException;

class InvalidMessageEnvelopeException extends UnexpectedValueException implements SpoolrailException
{
    public static function malformedJson(JsonException $previous): self
    {
        return new self(
            'The message envelope must contain valid JSON.',
            previous: $previous,
        );
    }

    public static function mustBeObject(): self
    {
        return new self('The message envelope must be a JSON object.');
    }

    public static function invalidId(): self
    {
        return new self('The message envelope must contain a valid UUID.');
    }

    public static function invalidType(): self
    {
        return new self(
            'The message envelope must contain a non-empty valid UTF-8 type of at most 255 bytes.',
        );
    }

    public static function payloadMustBeArray(): self
    {
        return new self('The message envelope must contain an array payload.');
    }

    public static function invalidTimestamp(): self
    {
        return new self('The message envelope must contain a valid ISO 8601 timestamp with a timezone.');
    }
}
