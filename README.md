# Spoolrail

Message broker library made native to Laravel for building resilient distributed systems.

## Installation

```bash
composer require spoolrail/spoolrail
```

You can publish the config file with:

```bash
php artisan vendor:publish --tag=spoolrail-config
```

## RabbitMQ

The RabbitMQ driver supports RabbitMQ 4.3 and later. Its AMQP client is optional so applications that select this driver also install:

```bash
composer require php-amqplib/php-amqplib:^3.7.4
```
