<?php
declare(strict_types=1);

namespace lingyiLib\DbMapper;

use Hyperf\Contract\ConnectionInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Pool\Connection as BaseConnection;
use Hyperf\Pool\Exception\ConnectionException;
use lingyiLib\DbMapper\Mapper\Connectors\ConnectionFactory;

use lingyiLib\DbMapper\Pool\DBPool;
use lingyiLib\DbMapper\Traits\DbConnection;
use Psr\Container\ContainerInterface;

class Connection extends BaseConnection implements ConnectionInterface
{
    use DbConnection;

    /**
     * @var DbPool
     */
    protected $pool;


    protected $connection;

    /**
     * @var array
     */
    protected $config;

    /**
     * @var StdoutLoggerInterface
     */
    protected $logger;

    /**
     * @var ConnectionFactory|mixed
     */
    protected $factory;

    protected $transaction = false;

    public function __construct(ContainerInterface $container, DbPool $pool, array $config)
    {
        parent::__construct($container, $pool);
        $this->config = $config;
        $this->factory = $container->get(ConnectionFactory::class);
        $this->logger = $container->get(StdoutLoggerInterface::class);
        $this->reconnect();
    }

    public function __call($name, $arguments)
    {
        return $this->connection->{$name}(...$arguments);
    }

    public function getActiveConnection()
    {
        if ($this->check()) {
            return $this;
        }

        if (! $this->reconnect()) {
            throw new ConnectionException('Connection reconnect failed.');
        }

        return $this;
    }

    public function reconnect(): bool
    {
        $this->close();
        $this->connection = $this->factory->make($this->config,$this->pool->getName());
        $this->lastUseTime = microtime(true);
        return true;
    }

    public function close(): bool
    {
        if ($this->connection instanceof \Doctrine\DBAL\Connection) {
            $this->connection->close();
        }

        unset($this->connection);

        return true;
    }

    public function release(): void
    {
        if ($this->isTransaction()) {
            $this->rollBack(0);
            $this->logger->error('Maybe you\'ve forgotten to  or rollback the MySQL transaction.');
        }

        parent::release();
    }

    public function setTransaction(bool $transaction): void
    {
        $this->transaction = $transaction;
    }

    public function isTransaction(): bool
    {
        return $this->transaction;
    }
}