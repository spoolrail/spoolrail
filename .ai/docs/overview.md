# Package Overview

Spoolrail is a Laravel message broker package with a transport-neutral API for publishing and consuming messages.

## Protected Outcomes

**Prefer duplicate delivery to message loss**: Drivers settle a source delivery only after its handoff succeeds. Failure or concurrency ambiguity may repeat a message; handoff idempotency must never discard an uncertain attempt. Exactly-once side effects remain the handler's responsibility.

**Keep publication ambiguity visible**: Retry broker-facing publication failures through one bounded package policy while preserving the prepared message identity. If acceptance remains unresolved after every attempt, report an unknown outcome. A successful publication confirms acceptance at least once, not subscription delivery or one accepted copy.

**Keep committed outbox intent recoverable**: With the outbox enabled, a publication belongs to its configured database transaction until commit and remains package responsibility until successful dispatch removes its row. Failed or ambiguous attempts retain that intent; uncertainty must not be resolved by discarding it.

**Keep outbox concurrency lane-scoped**: Dispatch may overlap only distinct stored connection-and-topic lanes. One lane remains commit-visible oldest first and a failed head still blocks its own later rows, so process allocation must never give a lane more than one owner within a run.

**Keep topology explicit**: Subscription declarations are the sole topology source. Publishing and consumption never create or reconcile resources. Synchronization preflights every referenced managed connection before applying any plan. Deletion remains a separate explicit operation.

**Topology recovery restarts reconciliation**: Recover non-destructive discovery and apply failures through one cross-driver synchronization retry, so recovery re-reads broker state instead of compounding hidden client attempts or continuing a stale plan. Destructive topology commands remain outside this policy.

**Isolate receive-side ownership**: Package-owned subscription resources use an explicit, stable application prefix. Operations that address those resources require it; publishing does not. Changing it selects a different physical resource namespace.

**Preserve in-flight compatibility**: Message identity remains stable through publication and consumption. Broker envelopes and Laravel Queue jobs may outlive a deployment, so later code must still understand their serialized forms and routing identities.

**Keep rename scopes distinct**: Former-name mappings reroute already-enqueued Laravel jobs only. Messages still buffered by the transport remain under the old subscription resource until deliberately drained.

**Preserve transport portability**: Shared message-size and logical resource-name constraints follow the most restrictive target transport. Do not relax them for one driver.

**Keep ordering capability explicit**: A portable ordering key requests the closest native grouping behavior from each driver. Ordering-capable transports preserve order only through broker-to-Queue handoff within one group; other drivers may accept the key without ordering effect, and Laravel Queue execution order remains outside the guarantee.

## Architectural Boundaries

**Publishing is subscription-independent**: Publishers do not declare or need to know subscriptions, and they do not create topology. Receiving applications must provision the required topic before publication begins.

**Publication policy stays above drivers**: The connection boundary prepares the portable publication once, then either sends it directly or stores it durably. Direct publication and outbox dispatch share bounded recovery above raw driver attempts, and outbox dispatch sends the stored envelope without restamping it.

**Consumption bridges to Laravel Queue**: A transport delivery becomes a Laravel Queue job; an asynchronous queue worker invokes the handler later. The `sync` Queue connection executes the handler inside the consumer's handoff and retains the same settle-after-success rule.

**Supervision selects processes, not messages**: The public consumer parent starts one clean PHP child per active subscription on one Spoolrail connection. Each child owns its transport receive loop and Queue handoff; the parent never receives, buffers, schedules, or settles deliveries.

**Outbox concurrency changes execution only**: Serial parent execution and finite concurrent workers share one lane-publication engine. The concurrent parent assigns complete lanes and supervises clean workers; it does not publish rows, coordinate per-row priority, or recreate publication and recovery behavior.

**Topology management is optional**: Drivers may support publishing and consumption without managing topology. Topology operations skip or reject those drivers instead of requiring every driver to own broker resources.
