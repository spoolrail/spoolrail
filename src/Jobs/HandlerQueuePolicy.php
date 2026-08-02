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
     * @param  class-string<MessageHandler>  $handlerClass
     *
     * @throws Throwable
     */
    public function apply(string $handlerClass, Message $message, HandleMessageJob $job): void
    {
        $handler = new ReflectionClass($handlerClass)->newInstanceWithoutConstructor();

        $job->tries = method_exists($handler, 'tries')
            ? $handler->tries()
            : $this->policyValue($handler, self::TRIES_ATTRIBUTE, 'tries');
        $job->backoff = method_exists($handler, 'backoff')
            ? $handler->backoff()
            : $this->policyValue($handler, self::BACKOFF_ATTRIBUTE, 'backoff');
        $job->maxExceptions = $this->policyValue($handler, self::MAX_EXCEPTIONS_ATTRIBUTE, 'maxExceptions');
        $job->timeout = $this->policyValue($handler, self::TIMEOUT_ATTRIBUTE, 'timeout');
        $job->failOnTimeout = $this->policyValue($handler, self::FAIL_ON_TIMEOUT_ATTRIBUTE, 'failOnTimeout') ?? false;
        $job->retryUntil = method_exists($handler, 'retryUntil') ? $handler->retryUntil() : null;
        $job->middleware = method_exists($handler, 'middleware') ? $handler->middleware($message) : [];
    }

    private function policyValue(object $handler, string $attributeClass, string $property): mixed
    {
        if (! class_exists($attributeClass)) {
            return $handler->{$property} ?? null;
        }

        $reflection = new ReflectionClass($handler);
        $resolvedAttribute = $this->nearestAttribute($reflection, $attributeClass);

        if ($resolvedAttribute === null) {
            return $handler->{$property} ?? null;
        }

        [$attribute, $declaringClass] = $resolvedAttribute;

        if ($this->propertyOverridesInheritedAttribute($handler, $reflection, $property, $declaringClass)) {
            return $handler->{$property};
        }

        return $this->attributeValue($attribute);
    }

    private function attributeValue(object $attribute): mixed
    {
        $values = get_object_vars($attribute);

        return $values === [] ? true : reset($values);
    }

    /**
     * @param  ReflectionClass<object>  $reflection
     * @return array{object, class-string}|null
     */
    private function nearestAttribute(
        ReflectionClass $reflection,
        string $attributeClass,
    ): ?array {
        try {
            do {
                $attributes = $reflection->getAttributes($attributeClass);

                if ($attributes !== []) {
                    return [$attributes[0]->newInstance(), $reflection->getName()];
                }

                foreach ($reflection->getTraits() as $trait) {
                    $attributes = $trait->getAttributes($attributeClass);

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
