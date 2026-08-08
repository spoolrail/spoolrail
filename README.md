# Spoolrail

> 🚧 On the rails and picking up steam toward 1.0 release.

Message broker library made native to Laravel for building resilient distributed systems.

## Documentation

See [spoolrail/docs](https://github.com/spoolrail/docs/blob/main/index.md). 

Better documentation coming soon once the first 3 planned drivers for 1.0 are out the door (see below).

## Supported Drivers

- 🐇 RabbitMQ
- ☁️ Google Pub/Sub (coming soon)
- 📦 AWS SNS/SQS (coming soon)

## Why a Message Broker?

A message broker lets different apps communicate without being directly connected. One app sends a message, the broker holds it, and another app picks it up later. Think of it like leaving a note for your coworker instead of waiting around to tell them in person.

This prevents the brittleness of direct HTTP calls, where one app has to wait for the other. So, whether the receiving app is offline or the system is overwhelmed by a sudden spike in traffic, messages wait safely in a queue until the system is ready to process them.

Message brokers are common in microservices and service-oriented architectures to handle async communication and keep things loosely coupled.

## Package Goals

Message broker systems are powerful, but that power can lead to complexity. There are no plans to chase every feature or configuration knob across drivers with this package. The goal is to focus on what’s essential for every broker implementation, delivering an elegant, driver-agnostic, and delightful API — even if that means leaving some bells and whistles behind.
