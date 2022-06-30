<?php
declare(strict_types=1);
namespace lingyiLib\DbMapper\Pool;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\ConnectionInterface;
use Hyperf\Pool\Pool;
use Hyperf\Utils\Arr;
use lingyiLib\DbMapper\Frequency;
use Psr\Container\ContainerInterface;
use lingyiLib\DbMapper\Connection;

class DBPool extends Pool
{
    protected $name;

    protected $config;

    public function __construct(ContainerInterface $container,$name='default')
    {
        $this->name = $name;
        $config = $container->get(ConfigInterface::class);
        $key = sprintf('databases.%s', $this->name);
        if (! $config->has($key)) {
            throw new \InvalidArgumentException(sprintf('config[%s] is not exist!', $key));
        }
        // Rewrite the `name` of the configuration item to ensure that the model query builder gets the right connection.
        $config->set("{$key}.name", $name);

        $this->config = $config->get($key);
        $options = Arr::get($this->config, 'pool', []);

        $this->frequency = make(Frequency::class, [$this]);

        parent::__construct($container, $options);
    }


    public function createConnection(): ConnectionInterface
    {
        return new Connection($this->container, $this, $this->config);
    }

    /**
     * @return mixed
     */
    public function getName()
    {
        return $this->name;
    }
}