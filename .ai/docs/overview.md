# Package Overview

## What This Package Does

Spoolrail provides a Laravel-native boundary for publishing logical messages to broker topics and consuming broker deliveries through Laravel Queue. Application code uses the same immutable `Message` model on both sides. Transport-specific receipt state and settlement behavior remain inside drivers.

The first package slice includes the permanent same-process `array` connection. It crosses the real JSON wire boundary and models topic fanout, independent subscription deliveries, acknowledgement after Queue push, and redelivery after handoff failure. It is intended for deterministic tests and local simulation, not persistence or cross-process transport.

## Architecture Map

- `SpoolrailManager` resolves, validates, lazily creates, caches, and forgets named application-facing connections. Each connection wraps a raw driver, and custom factories receive connection identity separately from unchanged driver configuration.
- `Facades\Spoolrail` exposes default or named connection publishing and registers application subscription definitions.
- `Connection` owns portable publication semantics by stamping through the immutable `Message` model and serializing through `MessageSerializer` before invoking its raw driver, and delegates subscription consumption to that driver.
- `Message` is the portable logical envelope. `MessageSerializer` owns only the exact JSON wire format shared by publication and consumption.
- `Subscriptions\SubscriptionRegistry` stores route definitions loaded from `routes/subscriptions.php`. Active names remain the only broker-facing identities; former names declared with `drainMessagesQueuedFor()` are indexed separately for queued-message execution. Route loading performs no transport work.
- `Contracts\Driver` defines the raw transport boundary. Each driver publishes serialized bodies, retains native receipt state during consumption, passes only the serialized delivery body to the handoff callback, and positively settles the source after that callback returns normally; callback and settlement failures stop consumption and propagate.
- `Drivers\ArrayDriver` stores independent serialized copies per matching subscription within one PHP process, reserves each body while its handoff runs, and restores it when the handoff throws.
- `Subscriptions\SubscriptionConsumer` resolves the subscription's Laravel Queue before source consumption, rejects a direct database Queue whose connection already has an open transaction, then hydrates each serialized body and pushes `Jobs\HandleMessageJob`; a normal callback return authorizes the driver to settle the source.
- `HandleMessageJob` stores the subscription name active at handoff and the hydrated message. At execution it resolves either that active definition or a replacement explicitly declared to drain its queued messages, then resolves the current `MessageHandler` through the container.
- `Console\ConsumeCommand` exposes `spoolrail:consume {subscription}` and lets terminal exceptions cross the command boundary without package-side retry or duplicate logging.
