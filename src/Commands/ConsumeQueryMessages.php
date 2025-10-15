<?php

namespace MHFereydouni\RabbitMQ\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use MHFereydouni\RabbitMQ\RabbitMQ;
use MHFereydouni\RabbitMQ\Query\RabbitMQQueryConsumer;
use MHFereydouni\RabbitMQ\Query\RabbitMQQueryMessage;
use MHFereydouni\RabbitMQ\Contracts\ShouldBeQueried;

class ConsumeQueryMessages extends Command
{
    protected $signature = 'rabbitmq:consume-queries';
    protected $description = 'Consume query messages from RabbitMQ and respond to them';

    /**
     * @var array<int, array{query: class-string, routing_key: string}>
     */
    private array $queries;

    public function __construct()
    {
        parent::__construct();

        // Load queries from config
        $this->queries = collect(config('rabbitmq.query-consumers', []))
            ->map(function ($query) {
                return [
                    'query' => $query['query'],
                    'routing_key' => $query['routing_key'] ?? '',
                ];
            })
            ->toArray();
    }

    public function handle(RabbitMQ $rabbitmq): int
    {
        $consumer = $rabbitmq->queryConsume();

        $queueName = config('app.name') . '_queries';

        // Bind each query to its exchange
        foreach ($this->queries as $query) {
            $consumer->bindTo(class_basename($query['query']), $query['routing_key']);
        }

        // Start consuming query messages
        $consumer->from($queueName, [$this, 'processQuery'])->receive();

        return Command::SUCCESS;
    }

    public function processQuery(array $payload, string $routingKey): void
    {
        $queryMeta = Arr::first($this->queries, function (array $query) use ($routingKey, $payload) {
            return $payload['query.name'] === class_basename($query['query'])
                && Str::is($query['routing_key'], $routingKey);
        });

        if (! $queryMeta) {
            Log::warning('Unknown query received', ['routing_key' => $routingKey]);
            return;
        }

        $queryClass = $queryMeta['query'];
        $message = $payload['__amqp_message'] ?? null;

        try {
            if (is_subclass_of($queryClass, ShouldBeQueried::class)) {
                $instance = resolve($queryClass, ['payload' => $payload]);
                $response = method_exists($instance, 'handle')
                    ? $instance->handle()
                    : ['error' => 'Query handler missing handle() method'];
            } else {
                $response = ['error' => 'Class does not implement ShouldBeQueried'];
            }
        } catch (\Throwable $e) {
            $response = [
                'error' => 'Exception during query handling',
                'message' => $e->getMessage(),
            ];

            Log::channel(config('rabbitmq.log-channel'))
                ->error('Query handling failed', [
                    'query' => $queryClass,
                    'routing_key' => $routingKey,
                    'payload' => $payload,
                    'exception' => $e->getMessage(),
                    'trace' => $e->getTraceAsString(),
                ]);
        }

        if ($message instanceof RabbitMQQueryMessage) {
            $message->reply($response);
        }
    }
}
