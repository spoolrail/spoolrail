<?php

declare(strict_types=1);

namespace Spoolrail\Spoolrail\SnsSqs;

use Illuminate\Support\Arr;
use JsonException;
use Spoolrail\Spoolrail\Exceptions\SnsSqsTopologyException;

class QueuePolicy
{
    private const string STATEMENT_ID = 'SpoolrailSnsPublish';

    public function withRoute(
        string $queue,
        ?string $policy,
        string $queueArn,
        string $topicArn,
    ): string {
        if ($policy === null || $policy === '') {
            return $this->encode([
                'Version' => '2012-10-17',
                'Statement' => [$this->statement($queueArn, $topicArn)],
            ]);
        }

        $document = $this->decode($queue, $policy);
        $statements = $this->statements($queue, $document);
        $required = $this->statement($queueArn, $topicArn);
        $existing = $this->spoolrailStatement($statements);

        if ($existing !== null) {
            $this->ensureStatementMatches($queue, $existing, $required);

            return $policy;
        }

        $statements[] = $required;
        $document['Statement'] = $statements;

        return $this->encode($document);
    }

    public function sourceTopicArn(
        string $queue,
        ?string $policy,
        string $queueArn,
    ): string {
        $document = $this->decode($queue, $policy);
        $statement = $this->requiredSpoolrailStatement($queue, $document);

        if (($statement['Resource'] ?? null) !== $queueArn) {
            throw SnsSqsTopologyException::missingQueueRoute($queue);
        }

        $topicArn = Arr::get($statement, 'Condition.ArnEquals.aws:SourceArn');

        if (! is_string($topicArn) || $topicArn === '') {
            throw SnsSqsTopologyException::missingQueueRoute($queue);
        }

        return $topicArn;
    }

    /**
     * @return array<string, mixed>
     */
    private function decode(string $queue, ?string $policy): array
    {
        if ($policy === null || $policy === '') {
            throw SnsSqsTopologyException::missingQueueRoute($queue);
        }

        try {
            $document = json_decode($policy, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            throw SnsSqsTopologyException::invalidQueuePolicy($queue);
        }

        return $this->object($queue, $document);
    }

    /**
     * @param  array<string, mixed>  $document
     * @return list<array<string, mixed>>
     */
    private function statements(string $queue, array $document): array
    {
        $statements = $document['Statement'] ?? [];

        if (! is_array($statements)) {
            throw SnsSqsTopologyException::invalidQueuePolicy($queue);
        }

        $statements = array_is_list($statements) ? $statements : [$statements];

        return array_map(
            fn (mixed $statement): array => $this->object($queue, $statement),
            $statements,
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function statement(string $queueArn, string $topicArn): array
    {
        return [
            'Sid' => self::STATEMENT_ID,
            'Effect' => 'Allow',
            'Principal' => ['Service' => 'sns.amazonaws.com'],
            'Action' => 'sqs:SendMessage',
            'Resource' => $queueArn,
            'Condition' => [
                'ArnEquals' => [
                    'aws:SourceArn' => $topicArn,
                ],
            ],
        ];
    }

    /**
     * @param  list<array<string, mixed>>  $statements
     * @return array<string, mixed>|null
     */
    private function spoolrailStatement(array $statements): ?array
    {
        foreach ($statements as $statement) {
            if (($statement['Sid'] ?? null) === self::STATEMENT_ID) {
                return $statement;
            }
        }

        return null;
    }

    /**
     * @param  array<string, mixed>  $document
     * @return array<string, mixed>
     */
    private function requiredSpoolrailStatement(string $queue, array $document): array
    {
        $statement = $this->spoolrailStatement($this->statements($queue, $document));

        if ($statement === null) {
            throw SnsSqsTopologyException::missingQueueRoute($queue);
        }

        return $statement;
    }

    /**
     * @param  array<string, mixed>  $existing
     * @param  array<string, mixed>  $required
     */
    private function ensureStatementMatches(
        string $queue,
        array $existing,
        array $required,
    ): void {
        if ($existing != $required) {
            throw SnsSqsTopologyException::conflictingQueuePolicy($queue);
        }
    }

    /**
     * @param  array<string, mixed>  $document
     */
    private function encode(array $document): string
    {
        return json_encode($document, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    /**
     * @return array<string, mixed>
     */
    private function object(string $queue, mixed $value): array
    {
        if (! is_array($value)) {
            throw SnsSqsTopologyException::invalidQueuePolicy($queue);
        }

        $object = [];

        foreach ($value as $key => $item) {
            if (! is_string($key)) {
                throw SnsSqsTopologyException::invalidQueuePolicy($queue);
            }

            $object[$key] = $item;
        }

        return $object;
    }
}
