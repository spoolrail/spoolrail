# Package Overview

Spoolrail is a Laravel message broker library that provides a unified interface for publishing and consuming messages across different transports.

## Protected Outcomes

**Message identity stability**: Messages are immutable once created and use UUID v7 identifiers. This prevents confusion about message identity when messages are serialized, transported, and deserialized across system boundaries. The UUID v7 requirement ensures time-ordering, which supports debugging and replay scenarios.

**Transport-level delivery guarantees**: The RabbitMQ driver uses publisher confirms and persistent delivery mode. This ensures messages are not lost in transit between publisher and broker, or between broker and consumer. The tradeoff is reduced throughput compared to non-persistent messaging.

**Topology ownership isolation**: The ownership prefix namespaces receive-side resources (queues) to prevent conflicts when multiple applications share a broker. Without this, applications could accidentally consume each other's messages or delete each other's queues.

**Safe subscription evolution**: The drain mechanism allows subscription renames without losing in-flight messages. When a subscription is renamed, the old name becomes a drain target that routes queued messages to the new subscription. This prevents message loss during deployment transitions.

**Handler behavior preservation**: Queue job attributes (tries, backoff, timeout, maxExceptions) are captured from handler classes at dispatch time, not at job creation. This ensures handler-defined retry behavior is preserved even when messages are queued for later processing.

**Publication semantics**: When broker confirmation times out, the outcome is ambiguous — Spoolrail cannot prove whether the message was accepted, so it does not retry automatically. Success confirms broker acceptance, not subscription delivery.

**Explicit topology management**: Subscription routes are the sole declaration source for broker topology. Publication and consumption never create or reconcile infrastructure implicitly. A preflight validation checks the complete declared topology before any creation; any incompatibility anywhere prevents creation everywhere. Destructive changes (deletion, recreation) remain explicit commands.

## Non-Obvious Boundaries

**Connection is a thin wrapper**: Connection delegates all transport operations to its driver and does not manage connection lifecycle. The driver owns the connection state and is responsible for reconnection and cleanup.

**ArrayDriver is simulation-only**: The array driver stores messages in memory and does not manage topology. It exists for testing and local development, not as a production transport.

**MessageEnvelope is internal**: The envelope encodes and decodes a specific JSON representation and is not designed for customization. Changing that representation breaks compatibility with all existing messages in the broker.

**SubscriptionConsumer bridges to Laravel queues**: The consumer does not invoke handlers directly. It decodes message envelopes and dispatches the resulting messages to Laravel's queue system, which then invokes handlers. This decouples message receipt from message processing.

**CanManageTopology is optional**: Drivers are not required to support topology management. The array driver does not implement CanManageTopology, and topology sync operations skip connections without that capability.

**Publishing is subscription-independent**: Publishing depends only on the logical topic and does not require the publishing application to declare or know about subscriptions. A publisher-only application assumes receiving applications have synchronized shared topics before publication begins. Publishing to a topic with no subscriptions succeeds.

## Cross-Component Compatibility Costs

**Message structure changes**: Modifying the Message class fields or the JSON envelope format breaks deserialization of all existing messages in the broker. Any change requires a migration strategy for in-flight messages.

**LogicalName pattern changes**: The pattern `/\\A[A-Za-z][A-Za-z0-9_-]{2,}\\z/` governs topic and subscription names. Changing it invalidates existing resource names in the broker and breaks applications using the old naming convention.

**Handoff contract changes**: The driver's consume method passes a serialized body string to the handoff closure. Changing this contract breaks all consumers and the SubscriptionConsumer bridge.

**OwnershipPrefix validation changes**: The prefix pattern `/\\A[A-Za-z][A-Za-z0-9_-]*\\z/` governs resource naming. Changing it breaks existing deployments that rely on the current prefix format.

**Handler queue policy changes**: Modifying how HandlerQueuePolicy captures attributes affects all message handlers. Handlers that rely on specific attribute behavior may need updates.

## Design Rationale

**Why UUID v7**: UUID v7 provides time-ordering, which supports debugging (messages can be sorted by creation time) and replay scenarios (messages can be replayed in order).

**Why millisecond timestamp precision**: The published_at timestamp uses millisecond precision (`Y-m-d\\TH:i:s.v\\Z`). This balances precision with storage efficiency and avoids microsecond-level noise that rarely matters for message ordering.

**Why 256 KiB envelope limit**: The Connection class enforces a maximum envelope size of 256 KiB (262,144 bytes) — the AWS SQS ceiling and smallest across supported transports. This shared limit prevents applications from discovering after switching drivers that previously accepted messages are too large for another transport.

**Why reject transactional database queues**: SubscriptionConsumer rejects Laravel's database queue when its connection has an open transaction. This prevents deadlocks that occur when the queue tries to insert a job while the transaction is still open. The tradeoff is that applications using database queues must commit or rollback before consuming.

**Why drain instead of direct rename**: Direct subscription rename would lose messages already queued under the old name. The drain mechanism preserves these messages by routing them to the new subscription, at the cost of additional complexity in the subscription registry.

**Why accept ambiguous publication outcomes**: When a publisher confirmation times out, Spoolrail cannot prove whether the broker accepted the message before the confirmation was lost. Retrying the original message would publish another envelope with the same logical ID but a new `publishedAt` timestamp, creating a duplicate. This ambiguity is surfaced rather than hidden behind automatic retry or a generic receipt that the driver cannot justify.
