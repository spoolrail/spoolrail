# Package Overview

## Protected Outcomes

**Keep completed queue handoffs idempotent and uncertain ones retryable.** Within the configured idempotency window, a known-complete handoff suppresses another Laravel job for the same message and subscription. If the handoff fails or Spoolrail cannot confirm that it completed, Spoolrail leaves the broker delivery unsettled. A later retry may enqueue another job. Laravel queue owns handler execution after the handoff, so its retries may repeat handler side effects.

**Keep publication ambiguity visible.** Broker-facing publication attempts share one bounded package policy and reuse the same prepared envelope. If any attempt may have reached the broker and the final outcome remains unresolved, report it as unknown. Success confirms broker acceptance at least once, not subscriber delivery or a single accepted copy.

**Do not discard committed outbox intent.** An outbox publication belongs to its configured database transaction and, after commit, remains package responsibility until dispatch success is known. Failed or ambiguous attempts retain it.

**Keep topology explicit.** Synchronization applies nothing until every referenced managed connection passes preflight. Retries re-read broker state. Deletion remains a separate explicit operation.

**Keep in-flight work compatible across deployments.** Preserve message and routing identities through publication and consumption. Newer code must continue to process older serialized broker envelopes and queued Laravel jobs.

**Preserve transport portability.** Shared message-size and logical-name limits follow the strictest supported transport. Stronger behavior in one driver does not expand the portable guarantee.
