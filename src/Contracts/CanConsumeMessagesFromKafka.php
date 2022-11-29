<?php

namespace Junges\Kafka\Contracts;

interface CanConsumeMessagesFromKafka
{
    /**
     * Creates a new ConsumerBuilder instance.
     *
     * @param  string  $brokers
     * @param  array  $topics
     * @param  string|null  $groupId
     * @return static
     */
    public static function create(string $brokers, array $topics = [], string $groupId = null): self;
}
