<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\Testing\Fakes;

use Carbon\CarbonImmutable;
use Closure;
use Illuminate\Support\Testing\Fakes\Fake;
use PHPUnit\Framework\Assert as PHPUnit;
use Spoolrail\Spoolrail\Message;

class SpoolrailFake implements Fake
{
    /**
     * @var list<array{
     *     topic: string,
     *     type: string,
     *     payload: array<array-key, mixed>,
     *     headers: array<string, string>
     * }>
     */
    private array $publications = [];

    public function connection(?string $name = null): self
    {
        return $this;
    }

    /**
     * @param  array<string, string>  $headers
     */
    public function publish(
        string $topic,
        Message $message,
        array $headers = [],
        ?string $orderingKey = null,
    ): Message {
        $this->publications[] = [
            'topic' => $topic,
            'type' => $message->type,
            'payload' => $message->payload,
            'headers' => $headers,
        ];

        return $message->withPublishedAt(CarbonImmutable::now('UTC'));
    }

    /**
     * @param  (Closure(array<array-key, mixed>, array<string, string>): bool)|int|null  $callback
     */
    public function assertPublished(
        string $topic,
        string $type,
        Closure|int|null $callback = null,
    ): void {
        if (is_int($callback)) {
            $published = $this->matchingPublications($topic, $type);

            PHPUnit::assertCount(
                $callback,
                $published,
                "Expected $callback [$type] messages published to [$topic], but found ".count($published).'.',
            );

            return;
        }

        PHPUnit::assertNotEmpty(
            $this->matchingPublications($topic, $type, $callback),
            "The expected [$type] message was not published to [$topic].",
        );
    }

    /**
     * @param  (Closure(array<array-key, mixed>, array<string, string>): bool)|null  $callback
     */
    public function assertNotPublished(
        string $topic,
        string $type,
        ?Closure $callback = null,
    ): void {
        PHPUnit::assertEmpty(
            $this->matchingPublications($topic, $type, $callback),
            "The unexpected [$type] message was published to [$topic].",
        );
    }

    public function assertNothingPublished(): void
    {
        $published = implode("\n- ", array_map(
            fn (array $publication): string => "[{$publication['type']}] on [{$publication['topic']}]",
            $this->publications,
        ));

        PHPUnit::assertEmpty(
            $this->publications,
            "The following messages were published unexpectedly:\n\n- $published\n",
        );
    }

    /**
     * @param  (Closure(array<array-key, mixed>, array<string, string>): bool)|null  $callback
     * @return list<array{
     *     topic: string,
     *     type: string,
     *     payload: array<array-key, mixed>,
     *     headers: array<string, string>
     * }>
     */
    private function matchingPublications(
        string $topic,
        string $type,
        ?Closure $callback = null,
    ): array {
        return array_values(array_filter(
            $this->publications,
            fn (array $publication): bool => $publication['topic'] === $topic
                && $publication['type'] === $type
                && (! $callback instanceof Closure || $callback($publication['payload'], $publication['headers'])),
        ));
    }
}
