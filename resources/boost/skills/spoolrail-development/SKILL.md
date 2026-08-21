---
name: spoolrail-development
description: Use when implementing or testing brokered messaging with Spoolrail in a Laravel application.
---

# Spoolrail Development

Verify APIs and behavior against the installed Spoolrail version rather than assuming them. Consult the [Spoolrail documentation](https://spoolrail.com) to configure and operate it.

Publication mode carries delivery-guarantee semantics. Treat the application's configured mode as deliberate, not a default to change in passing.

Spoolrail's responsibility ends at the queue handoff. It settles the broker delivery once the Laravel queue accepts the message, and from there Laravel queue owns handler execution — its retries, backoff, timeouts, and failures.

Use `Spoolrail::fake()` for producer-side publication assertions, and an unfaked `array` transport when the test depends on transport-portability validation, routing, or handler behavior.
