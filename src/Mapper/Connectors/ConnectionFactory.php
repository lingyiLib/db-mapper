<?php
declare(strict_types=1);

namespace lingyiLib\DbMapper\Mapper\Connectors;

use Doctrine\DBAL\Connections\PrimaryReadReplicaConnection;
use Hyperf\Utils\Arr;
use lingyiLib\DbMapper\Mapper\Config;
use Psr\Container\ContainerInterface;

class ConnectionFactory
{
    /**
     * The IoC container instance.
     *
     * @var ContainerInterface
     */
    protected $container;

    /**
     * Create a new connection factory instance.
     */
    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    /**
     * Establish a PDO connection based on the configuration.
     *
     * @param string $name
     * @return \Doctrine\DBAL\Connection
     */
    public function make(array $config, $name = null)
    {
        //主从配置
        if (isset($config['read'])) {
            return $this->createReadWriteConnection($config, $name);
        }
        //常规配置
        return $this->createSingleConnection($config, $name);
    }

    /**
     * Parse and prepare the database configuration.
     *
     * @param null $name
     * @return array
     */
    protected function parseConfig(array $config, $name = null)
    {
        $config = $name ? Arr::get($config, $name, []) : $config;
        $driver = Arr::get($config,'driver') ? 'pdo_'.$config['driver'] : '';
        return array_filter([
            'host' => $config['host'] ?? 'localhost',
            'port' => $config['port'] ?? 3306,
            'user' => $config['username'] ?? 'root',
            'password' => $config['password'] ?? 'root',
            'dbname' => $config['database'] ?? '',
            'charset' => $config['charset'] ?? '',
            'collation' => $config['collation'] ?? '',
            'driver'=>$driver,
        ],function ($item){
            return $item != null || $item === 0;
        });
    }

    /**
     * Create a single database connection instance.
     *
     * @return \Doctrine\DBAL\Connection
     */
    protected function createSingleConnection(array $config, $name)
    {
        $cfg = $this->container->get(Config::class);
        if(!$cfg->connection($name)){
            return $cfg->addConnection($name, $this->parseConfig($config));
        }
       return $cfg->connection($name);
    }

    /**
     * Create a single database connection instance.
     *
     * @return \Doctrine\DBAL\Connection
     */
    protected function createReadWriteConnection(array $config, $name)
    {
        $cfg = new Config();
        return $cfg->addConnection($name, [
            'primary' => $this->getWriteConfig($config),
            'replica' => [
                $this->getReadConfig($config),
            ],
            'wrapperClass' => PrimaryReadReplicaConnection::class,
            'driver' => 'pdo_' . $config['driver'],
        ]);
    }

    /**
     * Get the read configuration for a read / write connection.
     *
     * @return array
     */
    protected function getReadConfig(array $config)
    {
        return $this->mergeReadWriteConfig(
            $this->parseConfig($config),
            $this->parseConfig($this->getReadWriteConfig($config, 'read'))
        );
    }

    /**
     * Get the read configuration for a read / write connection.
     *
     * @return array
     */
    protected function getWriteConfig(array $config)
    {
        return $this->mergeReadWriteConfig(
            $this->parseConfig($config),
            $this->parseConfig($this->getReadWriteConfig($config, 'write'))
        );
    }

    /**
     * Get a read / write level configuration.
     *
     * @param string $type
     * @return array
     */
    protected function getReadWriteConfig(array $config, string $type)
    {
        return isset($config[$type][0])
            ? Arr::random($config[$type])
            : $config[$type];
    }

    /**
     * Merge a configuration for a read / write connection.
     *
     * @return array
     */
    protected function mergeReadWriteConfig(array $config, array $merge)
    {
        return Arr::except(array_merge($config, $merge), ['read', 'write']);
    }
}
