<?php

namespace Junges\Kafka\Contracts;

use Closure;

interface CanConsumeMessages
{
    /**
     * Consume messages from a kafka topic in loop.
     *
     * @throws \RdKafka\Exception|\Carbon\Exceptions\Exception|\Junges\Kafka\Exceptions\KafkaConsumerException
     */
    public function consume(): void;

    /** Requests the consumer to stop after it's finished processing any messages to allow graceful exit. */
    public function stopConsuming(): void;

    /**
     * Will cancel the stopConsume request initiated by calling the stopConsume method
     */
    public function cancelStopConsume(): void;

    /**
     * Count the number of messages consumed by this consumer
     */
    public function consumedMessagesCount(): int;

    /** Defines a callable that will run when the consumer stops consuming messages. */
    public function onStopConsuming(?Closure $onStopConsuming = null): self;
}
