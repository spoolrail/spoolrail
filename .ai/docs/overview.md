# Package Overview

## What This Package Does

Spoolrail provides a Laravel-native boundary for publishing logical messages to broker topics and consuming broker deliveries through Laravel Queue. Application code uses the same immutable `Message` model on both sides. Transport-specific receipt state and settlement behavior remain inside drivers.

The first package slice includes the permanent same-process `array` connection. It crosses the real JSON wire boundary and models topic fanout, independent subscription deliveries, acknowledgement after Queue push, and redelivery after handoff failure. It is intended for deterministic tests and local simulation, not persistence or cross-process transport.

## Architecture Map

- `SpoolrailManager` resolves, validates, lazily creates, caches, and forgets named application-facing connections. It keeps the configured default connection name distinct from resolved `Connection` instances. Each connection wraps a raw driver, and custom factories receive connection identity separately from unchanged driver config.
- `Facades\Spoolrail` exposes default or named connection publishing and registers application subscription definitions.
- `Connection` owns portable publication semantics by stamping through the immutable `Message` model and serializing through `MessageSerializer` before invoking its raw driver, and delegates subscription consumption to that driver.
- `Message` is the portable logical envelope. `MessageSerializer` owns only the exact JSON wire format shared by publication and consumption.
- `Subscriptions\SubscriptionRegistry` stores route definitions loaded from `routes/subscriptions.php`. Handlers must be concrete `MessageHandler` classes. Active names remain the only broker-facing identities; former names declared with `drainMessagesQueuedFor()` become queued-message aliases that resolve to the current active definition. Route loading performs no transport work.
- `Contracts\Driver` defines the raw transport boundary. Each driver publishes serialized bodies, retains native receipt state during consumption, passes only the serialized delivery body to the handoff callback, and positively settles the source after that callback returns normally. Callback failures stop consumption and propagate unchanged; settlement failures stop consumption and report that the successful handoff may be repeated.
- `Drivers\ArrayDriver` stores independent serialized copies per matching subscription within one PHP process, reserves each body while its handoff runs, and restores it when the handoff throws.
- `Subscriptions\SubscriptionConsumer` resolves the subscription's Laravel Queue before source consumption, rejects a direct database Queue whose connection already has an open transaction, then hydrates each serialized body, asks `Jobs\HandlerQueuePolicy` to capture the concrete handler's Queue policy and middleware, and pushes `Jobs\HandleMessageJob`; a normal callback return authorizes the driver to settle the source.
- `Jobs\HandleMessageJob` carries the Queue policy and middleware fixed at handoff together with the subscription name and hydrated message. At execution it resolves either that active definition or a replacement explicitly declared to drain its queued messages, then resolves the current `MessageHandler` through the container.
- `Topology` owns the portable grammar for logical topic and subscription names, resolves the physical-resource ownership prefix, and coordinates topology changes. `Topology\SyncTopology` groups active declarations by connection name, preflights every package-managed topology before applying any plan, and reports managed and unmanaged connection names separately. `RabbitMq\RabbitMqTopology` maps each logical topic and subscription to the required exchange, queue, and binding, and exposes undeclared physical subscription resource names for deletion.
- `Console\ConsumeCommand` exposes `spoolrail:consume {subscription}` and lets terminal exceptions cross the command boundary without package-side retry or duplicate logging.
