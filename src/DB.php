<?php

namespace lingyiLib\DbMapper;

use Hyperf\Utils\ApplicationContext;
use Psr\Container\ContainerInterface;
use lingyiLib\DbMapper\Mapper\Locator;
use Closure;

/**
 * @method static void beginTransaction()
 * @method static void commit()
 * @method static void rollBack()
 * @package lingyiLib\DbMapper
 */
class DB
{
    private $container;

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    public static function __callStatic($methodName, $arguments)
    {
        $db = ApplicationContext::getContainer()->get(Db::class);
        return $db->connection()->{$methodName}(...$arguments);
    }

    public static function mapper($entity)
    {
        $locator = new locator(ApplicationContext::getContainer());
        return $locator->Mapper($entity);
    }

    public function connection($connectionName='default')
    {
        return $this->container->get(ConnectionResolver::class)->connection($connectionName);
    }
}