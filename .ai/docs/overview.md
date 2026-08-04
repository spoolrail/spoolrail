# Package Overview

Spoolrail is a Laravel message broker package with a transport-neutral API for publishing and consuming messages.

## Protected Outcomes

**Prefer duplicate delivery to message loss**: Drivers settle a source delivery only after its handoff succeeds. Failure or concurrency ambiguity may repeat a message; bounded deduplication must never discard an uncertain attempt. Exactly-once side effects remain the handler's responsibility.

**Keep publication ambiguity visible**: When broker acceptance cannot be proven, report an unknown outcome instead of retrying automatically. A successful broker publication confirms acceptance, not subscription delivery.

**Keep topology explicit**: Subscription declarations are the sole topology source. Publishing and consumption never create or reconcile resources. Synchronization preflights every referenced managed connection before applying any plan, and deletion remains explicit.

**Isolate receive-side ownership**: Package-owned subscription resources use an explicit, stable application prefix. Operations that address those resources require it; publishing does not. Changing it selects a different physical resource namespace.

**Preserve in-flight compatibility**: Message identity remains stable through publication and consumption. Broker envelopes and Laravel Queue jobs may outlive a deployment, so later code must still understand their serialized forms and routing identities.

**Keep rename scopes distinct**: Former-name mappings reroute already-enqueued Laravel jobs only. Messages still buffered by the transport remain under the old subscription resource until deliberately drained.

**Preserve transport portability**: Shared message-size and logical resource-name constraints follow the most restrictive target transport. Do not relax them for one driver.

## Architectural Boundaries

**Publishing is subscription-independent**: Publishers do not declare or need to know subscriptions, and they do not create topology. Receiving applications must provision the required topic before publication begins.

**Consumption bridges to Laravel Queue**: A transport delivery becomes a Laravel Queue job; the queue worker invokes the handler later.

**Topology management is optional**: Drivers may support publishing and consumption without managing topology. Topology operations skip or reject those drivers instead of requiring every driver to own broker resources.
