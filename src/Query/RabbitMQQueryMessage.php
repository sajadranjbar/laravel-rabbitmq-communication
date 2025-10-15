<?php

namespace MHFereydouni\RabbitMQ\Query;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Ramsey\Uuid\Uuid;
use Exception;

class RabbitMQQueryMessage
{
    private AMQPChannel $channel;
    private array $payload = [];
    private string $exchange;
    private string $routingKey = '';
    private string $replyTo;
    private string $correlationId;
    private bool $persistent = false;

    public function __construct(AMQPChannel $channel)
    {
        $this->channel = $channel;
        $this->correlationId = Uuid::uuid4()->toString();
        $this->replyTo = '';
    }

    public function withPayload(array $payload): self
    {
        $this->payload = $payload;
        return $this;
    }

    public function persistent(): self
    {
        $this->persistent = true;
        return $this;
    }

    public function viaExchange(string $exchange): self
    {
        $this->exchange = $exchange;
        return $this;
    }

    public function route(string $routingKey): self
    {
        $this->routingKey = $routingKey;
        return $this;
    }

    public function publishAndWait(float $timeout = 5.0): array
    {
        // استفاده از queue ثابت برای پاسخ‌ها
        $this->replyTo = 'query.responses';

        // مطمئن شو queue وجود داره
        $this->channel->queue_declare(
            $this->replyTo,
            false,
            true,   // durable
            false,
            false
        );

        $response = null;

        $callback = function (AMQPMessage $message) use (&$response) {
            if ($message->get('correlation_id') === $this->correlationId) {
                $response = json_decode($message->getBody(), true);
            }
        };

        $this->channel->basic_consume(
            $this->replyTo,
            '',
            false,
            true,
            false,
            false,
            $callback
        );

        $msg = new AMQPMessage(json_encode($this->payload), $this->properties());
        $this->channel->basic_publish($msg, $this->exchange, $this->routingKey);

        $start = microtime(true);
        while ($response === null) {
            $this->channel->wait(null, false, $timeout);
            if ((microtime(true) - $start) > $timeout) {
                throw new Exception("Query timed out after {$timeout} seconds");
            }
        }

        return $response;
    }


    public function reply(array $response): void
    {
        $msg = new AMQPMessage(
            json_encode($response),
            ['correlation_id' => $this->correlationId]
        );
        $this->channel->basic_publish($msg, '', $this->replyTo);
    }

    private function properties(): array
    {
        $props = [
            'reply_to' => $this->replyTo,
            'correlation_id' => $this->correlationId,
        ];

        if ($this->persistent) {
            $props['delivery_mode'] = AMQPMessage::DELIVERY_MODE_PERSISTENT;
        }

        return $props;
    }
}
