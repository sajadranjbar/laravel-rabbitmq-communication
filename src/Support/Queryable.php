<?php

namespace MHFereydouni\RabbitMQ\Support;

use Exception;
use MHFereydouni\RabbitMQ\RabbitMQDispatcher;

trait Queryable
{
    /**
     * send Query by RabbitMQ
     *
     * @throws Exception
     */
    public function send(float $timeout = 5.0): array
    {
        /** @var RabbitMQDispatcher $rabbitmqDispatcher */
        $rabbitmqDispatcher = resolve(RabbitMQDispatcher::class);

        return $rabbitmqDispatcher
            ->query(
                $this->getExchange(),
                $this->getRoutingKey(),
                $this->getPayload(),
                $timeout
            );
    }
}
