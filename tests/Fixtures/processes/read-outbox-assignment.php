<?php

declare(strict_types=1);

$input = stream_get_contents(STDIN);

fwrite(STDOUT, json_encode([
    'arguments' => array_slice($argv, 1),
    'input' => $input,
], JSON_THROW_ON_ERROR));
