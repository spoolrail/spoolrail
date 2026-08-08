<?php

declare(strict_types=1);

fwrite(STDOUT, ($argv[1] ?? '').' '.($argv[2] ?? '')."\n");
fflush(STDOUT);

while (true) {
    usleep(100_000);
}
