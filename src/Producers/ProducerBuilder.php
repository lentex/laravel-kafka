<?php

namespace Junges\Kafka\Producers;

use Junges\Kafka\Concerns\InteractsWithConfigCallbacks;
use Junges\Kafka\Config\Config;
use Junges\Kafka\Config\Sasl;
use Junges\Kafka\Contracts\CanProduceMessages;
use Junges\Kafka\Contracts\KafkaProducerMessage;
use Junges\Kafka\Contracts\MessageSerializer;

class ProducerBuilder implements CanProduceMessages
{
    use InteractsWithConfigCallbacks;

    private array $options = [];
    private KafkaProducerMessage $message;
    private MessageSerializer $serializer;
    private ?Sasl $saslConfig = null;
    private ?string $topic = null;
    private ?string $brokers;

    public function __construct()
    {
        /** @var KafkaProducerMessage $message */
        $message = app(KafkaProducerMessage::class);
        $this->message = $message->create();
        $this->serializer = app(MessageSerializer::class);
        $this->brokers = $broker ?? config('kafka.brokers');
    }

    /** Return a new Junges\Commit\ProducerBuilder instance */
    public static function create(array $config): CanProduceMessages
    {
        return (new static())
            ->withBrokers($config['brokers'])
            ->withDebugEnabled($config['debug'])
            ->withConfigOption('compression.codec', $config['compression'])
            ->withConfigOptions($config['options']);
    }

    /** Set the brokers to be used */
    public function withBrokers(string $brokers): CanProduceMessages
    {
        $this->brokers = $brokers;

        return $this;
    }

    /** Set the topic to publish the message. */
    public function onTopic(string $topic): CanProduceMessages
    {
        $this->topic = $topic;

        return $this;
    }

    /** Set the given configuration option with the given value on KafkaProducer. */
    public function withConfigOption(string $name, mixed $option): CanProduceMessages
    {
        $this->options[$name] = $option;

        return $this;
    }

    /** Sets the given configuration options based on given key/value array. */
    public function withConfigOptions(array $options): CanProduceMessages
    {
        foreach ($options as $name => $value) {
            $this->withConfigOption($name, $value);
        }

        return $this;
    }

    /** Set the message headers. */
    public function withHeaders(array $headers = []): CanProduceMessages
    {
        $this->message->withHeaders($headers);

        return $this;
    }

    /** Set the message key. */
    public function withKafkaKey(string $key): CanProduceMessages
    {
        $this->message->withKey($key);

        return $this;
    }

    /** Set a message array key.*/
    public function withBodyKey(string $key, mixed $message): CanProduceMessages
    {
        $this->message->withBodyKey($key, $message);

        return $this;
    }

    /** Sets the entire Message to be produced. */
    public function withMessage(KafkaProducerMessage $message): CanProduceMessages
    {
        $this->message = $message;

        return $this;
    }

    /** Enables debug */
    public function withDebugEnabled(bool $enabled = true): CanProduceMessages
    {
        if ($enabled) {
            $this->withConfigOptions([
                'log_level' => LOG_DEBUG,
                'debug' => 'all',
            ]);
        } else {
            unset($this->options['log_level']);
            unset($this->options['debug']);
        }

        return $this;
    }

    /** Set the Sasl configuration. */
    public function withSasl(string $username, string $password, string $mechanisms, string $securityProtocol = 'SASL_PLAINTEXT'): CanProduceMessages
    {
        $this->saslConfig = new Sasl(
            username: $username,
            password: $password,
            mechanisms: $mechanisms,
            securityProtocol: $securityProtocol
        );

        return $this;
    }

    /**
     * Specify which class should be used to serialize messages.
     *
     * @param MessageSerializer $serializer
     * @return CanProduceMessages
     */
    public function usingSerializer(MessageSerializer $serializer): CanProduceMessages
    {
        $this->serializer = $serializer;

        return $this;
    }

    /** Disables debug on kafka producer. */
    public function withDebugDisabled(): CanProduceMessages
    {
        return $this->withDebugEnabled(false);
    }

    /**
     * Returns the message where the message should be published.
     *
     * @return string
     */
    public function getTopic(): string
    {
        return $this->topic;
    }

    /**
     * Produces the message on Kafka.
     *
     * @return bool
     */
    public function send(): bool
    {
        $producer = $this->build();

        return $producer->produce($this->message);
    }

    /**
     * Send a message batch to Kafka.
     *
     * @param  \Junges\Kafka\Producers\MessageBatch  $messageBatch
     * @throws \Junges\Kafka\Exceptions\CouldNotPublishMessage
     * @return int
     */
    public function sendBatch(MessageBatch $messageBatch): int
    {
        $producer = $this->build();

        return $producer->produceBatch($messageBatch);
    }

    /**
     * Build the Producer with the specified configuration options.
     *
     * @return \Junges\Kafka\Producers\Producer
     */
    private function build(): Producer
    {
        $conf = new Config(
            broker: $this->brokers,
            topics: [$this->getTopic()],
            securityProtocol: $this->saslConfig?->getSecurityProtocol(),
            sasl: $this->saslConfig,
            customOptions: $this->options,
            callbacks: $this->callbacks,
        );

        return app(Producer::class, [
            'config' => $conf,
            'topic' => $this->topic,
            'serializer' => $this->serializer,
        ]);
    }
}
