<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Jobs;

use Exception;
use Illuminate\Queue\Attributes\Backoff;
use Illuminate\Queue\Attributes\FailOnTimeout;
use Illuminate\Queue\Attributes\MaxExceptions;
use Illuminate\Queue\Attributes\Timeout;
use Illuminate\Queue\Attributes\Tries;
use ReflectionClass;
use Spoolrail\Spoolrail\Contracts\MessageHandler;
use Spoolrail\Spoolrail\Message;
use Throwable;

class HandlerQueuePolicy
{
    private const string TRIES_ATTRIBUTE = Tries::class;

    private const string BACKOFF_ATTRIBUTE = Backoff::class;

    private const string MAX_EXCEPTIONS_ATTRIBUTE = MaxExceptions::class;

    private const string TIMEOUT_ATTRIBUTE = Timeout::class;

    private const string FAIL_ON_TIMEOUT_ATTRIBUTE = FailOnTimeout::class;

    /**
     * @param  class-string<MessageHandler>  $handler
     *
     * @throws Throwable
     */
    public function apply(string $handler, Message $message, HandleMessageJob $job): void
    {
        $instance = new ReflectionClass($handler)->newInstanceWithoutConstructor();

        $job->tries = method_exists($instance, 'tries')
            ? $instance->tries()
            : $this->attributeOrProperty($instance, self::TRIES_ATTRIBUTE, 'tries');
        $job->backoff = method_exists($instance, 'backoff')
            ? $instance->backoff()
            : $this->attributeOrProperty($instance, self::BACKOFF_ATTRIBUTE, 'backoff');
        $job->maxExceptions = $this->attributeOrProperty($instance, self::MAX_EXCEPTIONS_ATTRIBUTE, 'maxExceptions');
        $job->timeout = $this->attributeOrProperty($instance, self::TIMEOUT_ATTRIBUTE, 'timeout');
        $job->failOnTimeout = $this->attributeOrProperty($instance, self::FAIL_ON_TIMEOUT_ATTRIBUTE, 'failOnTimeout') ?? false;
        $job->retryUntil = method_exists($instance, 'retryUntil') ? $instance->retryUntil() : null;
        $job->middleware = method_exists($instance, 'middleware') ? $instance->middleware($message) : [];
    }

    private function attributeOrProperty(object $handler, string $attribute, string $property): mixed
    {
        if (! class_exists($attribute)) {
            return $handler->{$property} ?? null;
        }

        $reflection = new ReflectionClass($handler);
        $defaultProperties = $reflection->getDefaultProperties();

        if (isset($handler->{$property}) && $handler->{$property} !== ($defaultProperties[$property] ?? null)) {
            return $handler->{$property};
        }

        $resolvedAttribute = $this->attributeInstance($reflection, $attribute);

        if ($resolvedAttribute === null) {
            return $handler->{$property} ?? null;
        }

        [$instance, $declaringClass] = $resolvedAttribute;

        if ($this->propertyOverridesInheritedAttribute($handler, $reflection, $property, $declaringClass)) {
            return $handler->{$property};
        }

        $values = get_object_vars($instance);

        return $values === [] ? true : reset($values);
    }

    /**
     * @param  ReflectionClass<object>  $reflection
     * @return array{object, class-string}|null
     */
    private function attributeInstance(
        ReflectionClass $reflection,
        string $attribute,
    ): ?array {
        try {
            do {
                $attributes = $reflection->getAttributes($attribute);

                if ($attributes !== []) {
                    return [$attributes[0]->newInstance(), $reflection->getName()];
                }

                foreach ($reflection->getTraits() as $trait) {
                    $attributes = $trait->getAttributes($attribute);

                    if ($attributes !== []) {
                        return [$attributes[0]->newInstance(), $reflection->getName()];
                    }
                }
            } while ($reflection = $reflection->getParentClass());
        } catch (Exception) {
            //
        }

        return null;
    }

    /**
     * @param  ReflectionClass<object>  $reflection
     * @param  class-string  $attributeDeclaringClass
     */
    private function propertyOverridesInheritedAttribute(
        object $handler,
        ReflectionClass $reflection,
        string $property,
        string $attributeDeclaringClass,
    ): bool {
        if (! $reflection->hasProperty($property)) {
            return false;
        }

        $reflectionProperty = $reflection->getProperty($property);

        return $reflectionProperty->isPublic()
            && $reflectionProperty->isInitialized($handler)
            && $reflectionProperty->getDeclaringClass()->isSubclassOf($attributeDeclaringClass);
    }
}
