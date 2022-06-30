<?php


namespace lingyiLib\DbMapper;

use Hyperf\Contract\ConnectionInterface;
use Hyperf\Utils\Context;
use Hyperf\Utils\Coroutine;
use lingyiLib\DbMapper\Pool\PoolFactory;
use Psr\Container\ContainerInterface;

class ConnectionResolver
{
    private $factory;

    public function __construct(ContainerInterface $container)
    {
        $this->factory = $container->get(PoolFactory::class);
    }

    public function connection($name)
    {
        $connection = null;
        $id = $this->getContextKey($name);
        if (Context::has($id)) {
            $connection = Context::get($id);
        }
        if (!$connection instanceof ConnectionInterface) {
            $connection = $this->factory->getPool($name);
            $pdo_pool = $connection->get();
            try {
                $connection = $pdo_pool->getConnection();
                Context::set($id, $connection);
            } finally {
                if (Coroutine::inCoroutine()) {
                    defer(function () use ($pdo_pool, $id) {
                        Context::set($id, null);
                        $pdo_pool->release();
                    });
                }
            }
        }
        return $connection;
    }

    private function getContextKey($name): string
    {
        return sprintf('dataDbMapper.connection.%s', $name);
    }
}