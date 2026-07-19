# Package Overview

## What This Package Does

Spoolrail provides a Laravel-native boundary for publishing logical messages to broker topics and consuming broker deliveries through Laravel Queue. Application code uses the same immutable `Message` model on both sides. Transport-specific receipt state and behavior belong behind driver and delivery contracts.

The first package slice includes the permanent same-process `array` connection. It crosses the real JSON wire boundary and models topic fanout, independent subscription deliveries, acknowledgement after Queue push, and redelivery after handoff failure. It is intended for deterministic tests and local simulation, not persistence or cross-process transport.

## Architecture Map

- `SpoolrailManager` resolves, validates, lazily creates, caches, and forgets named application-facing connections. Each connection wraps a raw driver, and custom factories receive connection identity separately from unchanged driver configuration.
- `Facades\Spoolrail` exposes default or named connection publishing and registers application subscription definitions.
- `Connection` owns portable publication semantics by stamping through the immutable `Message` model and serializing through `Serialization\MessageSerializer` before invoking its raw driver.
- `Message` is the portable logical envelope. `Serialization\MessageSerializer` owns only the exact JSON wire format shared by publication and consumption.
- `Subscriptions\SubscriptionRegistry` stores route definitions loaded from `routes/subscriptions.php`. Route loading performs no transport work.
- `Contracts\Driver`, `ConsumableDriver`, and `Delivery` define the raw transport boundary. Drivers publish serialized bodies and own their native consumption lifecycle while source acknowledgement remains explicit.
- `Drivers\ArrayDriver` stores independent serialized copies per matching subscription within one PHP process and drains each subscription through a delivery callback.
- `Consumers\SubscriptionConsumer` hydrates a delivery, pushes `Jobs\HandleMessageJob` directly to the subscription's Laravel Queue connection and queue, then acknowledges the source delivery.
- `HandleMessageJob` stores the stable subscription name and hydrated message. At execution it resolves the current subscription and container-resolved `MessageHandler`.
- `Console\ConsumeCommand` exposes `spoolrail:consume {subscription}` and lets terminal exceptions cross the command boundary without package-side retry or duplicate logging.
