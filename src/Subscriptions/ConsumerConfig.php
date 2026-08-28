<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Subscriptions;

use Illuminate\Contracts\Config\Repository;
use Spoolrail\Spoolrail\Exceptions\InvalidConfigException;

/** @internal */
class ConsumerConfig
{
    public function __construct(private Repository $config) {}

    public function processes(): int
    {
        return $this->positiveInteger('processes', 1);
    }

    public function idleWaitMilliseconds(): int
    {
        return $this->positiveInteger('idle_wait_milliseconds', 100);
    }

    private function positiveInteger(string $setting, int $default): int
    {
        $value = $this->config->get("spoolrail.consumer.$setting", $default);

        if (! is_int($value) || $value < 1) {
            throw InvalidConfigException::invalidConsumerSetting($setting);
        }

        return $value;
    }
}
