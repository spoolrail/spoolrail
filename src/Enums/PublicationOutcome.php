<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Enums;

enum PublicationOutcome
{
    case NotSent;
    case Rejected;
    case Unknown;
}
