<?php

namespace MHFereydouni\RabbitMQ\Query;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitMQQueryConsumer
{
    private AMQPChannel $channel;

    public function __construct(AMQPChannel $channel)
    {
        $this->channel = $channel;
    }

    public function from(string $queue, callable $handle): void
    {
        $this->channel->basic_qos(null, 1, null);

        $this->channel->basic_consume(
            $queue,
            '',
            false,
            false,
            false,
            false,
            function (AMQPMessage $message) use ($handle) {
                try {
                    $payload = json_decode($message->getBody(), true);
                    $payload['__amqp_message'] = $message;

                    call_user_func_array($handle, [
                        $payload,
                        $message->getRoutingKey(),
                    ]);

                    $message->ack();
                } catch (\Throwable $e) {
                    throw new \Exception(
                        "Error processing query message: " . $e->getMessage(),
                        0,
                        $e
                    );
                }
            }
        );

        while ($this->channel->is_open()) {
            $this->channel->wait();
        }
    }

    public function __destruct()
    {
        $this->channel->close();
    }
}
