<?php

namespace MHFereydouni\RabbitMQ\Support;

interface ShouldBeQueried
{
    public function getExchange(): string;
    public function getRoutingKey(): string;
    public function getPayload(): array;
}
