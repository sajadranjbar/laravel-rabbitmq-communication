<?php

namespace MHFereydouni\RabbitMQ\Commands;

use App\Providers\QueryServiceProvider;
use App\Queries\QueryInterface;
use Illuminate\Console\Command;
use MHFereydouni\RabbitMQ\RabbitMQ;
use MHFereydouni\RabbitMQ\Support\ShouldBeQueried;

class DeclareQueryExchanges extends Command
{
    protected $signature = 'rabbitmq:declare-queries-exchanges';
    protected $description = 'Declare RabbitMQ exchanges for all Queries';

    public function handle(RabbitMQ $rabbitmq): int
    {
        /** @var QueryServiceProvider $provider */
        $provider = new QueryServiceProvider(app());
        collect($provider->queryExchanges)
            ->filter(function (string $queryClass) {
                return in_array(ShouldBeQueried::class, class_implements($queryClass));
            })
            ->each(function (string $queryClass) use ($rabbitmq) {
                $rabbitmq
                    ->exchange()
                    ->durable()
                    ->type($this->determineExchangeType($queryClass))
                    ->name(class_basename($queryClass))
                    ->declare();

                $this->info('Declared ' . class_basename($queryClass));
                $this->newLine();
            });

        return Command::SUCCESS;
    }

    private function determineExchangeType(string $queryClass): string
    {
        $reflection = new \ReflectionClass($queryClass);

        if (!$reflection->hasProperty('exchangeType')) {
            return 'fanout';
        }

        $property = $reflection->getProperty('exchangeType');
        $property->setAccessible(true);

        return $property->getValue();
    }

}
