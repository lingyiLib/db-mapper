<?php
namespace lingyiLib\DbMapper\Mapper;

use Hyperf\Contract\ConnectionInterface;
use Hyperf\Utils\ApplicationContext;
use lingyiLib\DbMapper\ConnectionResolver;
use lingyiLib\DbMapper\DB;
use Psr\Container\ContainerInterface;

/**
 * @package lingyiLib\Database\Spot
 */
class Locator
{
    private $db;
    private $mapper = [];

    public function __construct()
    {
        $this->db = ApplicationContext::getContainer()->get(Db::class);
    }

    /**
     * Get config class mapper was instantiated with
     * @return \Doctrine\DBAL\Connection
     */
    public function getConnection($connectionName)
    {
        return $this->db->connection($connectionName);
    }

    /**
     * Get mapper for specified entity
     *
     * @param  string      $entityName Name of Entity object to load mapper for
     * @return \lingyiLib\DbMapper\Mapper\Mapper
     */
    public function mapper($entityName)
    {
        if (!isset($this->mapper[$entityName])) {
            // Get custom mapper, if set
            $mapper = $entityName::mapper();
            // Fallback to generic mapper
            if ($mapper === false) {
                $mapper = 'lingyiLib\DbMapper\Mapper\Mapper';
            }
            $this->mapper[$entityName] = new $mapper($this, $entityName);
        }

        return $this->mapper[$entityName];
    }
}
