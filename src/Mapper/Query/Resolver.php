<?php
namespace lingyiLib\DbMapper\Mapper\Query;

use Closure;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Utils\ApplicationContext;
use lingyiLib\DbMapper\Exception\QueryException;
use lingyiLib\DbMapper\Mapper\Connectors\ConnectionFactory;
use lingyiLib\DbMapper\Mapper\DetectsLostConnections;
use lingyiLib\DbMapper\Mapper\Mapper;
use lingyiLib\DbMapper\Mapper\Query;
use Doctrine\DBAL\Schema\Table;
use lingyiLib\DbMapper\Mapper\Relation\BelongsTo;

/**
 * Main query resolver
 *
 * @package lingyiLib\DbMapper\Mapper
 * @author Vance Lucas <vance@vancelucas.com>
 */
class Resolver
{
    use DetectsLostConnections;
    /**
     * @var \lingyiLib\DbMapper\Mapper\Mapper
     */
    protected $mapper;

    protected $factory;

    protected $logger;

    protected $_noQuote;

    /**
     * Constructor Method
     *
     * @param \lingyiLib\DbMapper\Mapper\Mapper $mapper
     */
    public function __construct(Mapper $mapper)
    {
        $this->mapper = $mapper;
        $this->factory = make(ConnectionFactory::class);
    }

    /**
     * Set field and value quoting on/off - maily used for testing output SQL
     * since quoting is different per platform
     *
     * @param bool $noQuote
     * @return $this
     */
    public function noQuote($noQuote = true)
    {
        $this->_noQuote = $noQuote;

        return $this;
    }

    /**
     * Migrate table structure changes to database
     *
     * @return bool
     * @throws \Doctrine\DBAL\Schema\SchemaException
     * @throws \lingyiLib\DbMapper\Mapper\Exception
     */
    public function migrate()
    {
        // Mapper knows currently set entity
        $entity = $this->mapper->entity();
        $table = $entity::table();
        $fields = $this->mapper->entityManager()->fields();
        $fieldIndexes = $this->mapper->entityManager()->fieldKeys();
        $connection = $this->mapper->connection();

        $schemaManager = $this->mapper->connection()->getSchemaManager();
        $tableObject = $schemaManager->listTableDetails($table);
        $tableObjects[] = $tableObject;
        $schema = new \Doctrine\DBAL\Schema\Schema($tableObjects);

        $tableColumns = $tableObject->getColumns();
        $tableExists = !empty($tableColumns);
        if ($tableExists) {
            // Update existing table
            $existingTable = $schema->getTable($table);
            $newSchema = $this->migrateCreateSchema();
            $queries = $schema->getMigrateToSql($newSchema, $connection->getDatabasePlatform());
        } else {
            // Create new table
            $newSchema = $this->migrateCreateSchema();
            $queries = $newSchema->toSql($connection->getDatabasePlatform());
        }

        // Execute resulting queries
        $lastResult = false;
        foreach ($queries as $sql) {
            $lastResult = $connection->exec($sql);
        }

        return $lastResult;
    }

    /**
     * Migrate create schema
     *
     * @return \Doctrine\DBAL\Schema\Schema
     */
    public function migrateCreateSchema()
    {
        $entityName = $this->mapper->entity();
        $table = $entityName::table();
        $fields = $this->mapper->entityManager()->fields();
        $fieldIndexes = $this->mapper->entityManager()->fieldKeys();

        $schema = new \Doctrine\DBAL\Schema\Schema();
        $table = $schema->createTable($this->escapeIdentifier($table));

        foreach ($fields as $field) {
            $fieldType = $field['type'];
            unset($field['type']);
            $table->addColumn($this->escapeIdentifier($field['column']), $fieldType, $field);
        }

        // PRIMARY
        if ($fieldIndexes['primary']) {
            $resolver = $this;
            $primaryKeys = array_map(function($value) use($resolver) { return $resolver->escapeIdentifier($value); }, $fieldIndexes['primary']);
            $table->setPrimaryKey($primaryKeys);
        }
        // UNIQUE
        foreach ($fieldIndexes['unique'] as $keyName => $keyFields) {
            $table->addUniqueIndex($keyFields, $this->escapeIdentifier($this->trimSchemaName($keyName)));
        }
        // INDEX
        foreach ($fieldIndexes['index'] as $keyName => $keyFields) {
            $table->addIndex($keyFields, $this->escapeIdentifier($this->trimSchemaName($keyName)));
        }
        // FOREIGN KEYS
        $this->addForeignKeys($table);

        return $schema;
    }

    /**
     * Find records with custom SQL query
     *
     * @param \lingyiLib\DbMapper\Mapper\Query $query SQL query to execute
     * @return \lingyiLib\DbMapper\Mapper\Entity\Collection
     * @throws \lingyiLib\DbMapper\Mapper\Exception
     */
    public function read(Query $query)
    {
        try {
            $stmt = $query->builder()->executeQuery();
        }catch(\Exception $e){
            $stmt = $this->tryAgainIfCausedByLostConnection(
                $e,
                function () use ($query) {
                    return $query->builder()->executeQuery();
                });
        }
        // Set PDO fetch mode
        $collection = $query->mapper()->collection($stmt->fetchAllAssociative(), $query->with());

        // Ensure statement is closed
        $stmt->free();
        return $collection;
    }

    /**
     * Create new row object with set properties
     *
     * @param string $table Table name
     * @param array $data Array of data to save in 'field' => 'value' format
     */
    public function create($table, array $data)
    {
        $connection = $this->mapper->connection();
        try {
            return $connection->insert($this->escapeIdentifier($table), $this->dataWithFieldAliasMappings($data));
        }catch(\Exception $e){
            return $this->tryAgainIfCausedByLostConnection(
                $e,
                function () use ($connection,$table,$data) {
                    return $connection->insert($this->escapeIdentifier($table), $this->dataWithFieldAliasMappings($data));
                });
        }
    }

    /**
     * Update
     *
     * @param string $table Table name
     * @param array $data Array of data for WHERE clause in 'field' => 'value' format
     * @param array $where
     * @return
     * @throws \lingyiLib\DbMapper\Mapper\Exception
     */
    public function update($table, array $data, array $where)
    {
        $connection = $this->mapper->connection();
        try{
            return $connection->update($this->escapeIdentifier($table), $this->dataWithFieldAliasMappings($data), $this->dataWithFieldAliasMappings($where));
        }catch(\Exception $e){
            return $this->tryAgainIfCausedByLostConnection(
                $e,
                function () use ($connection,$table,$data,$where) {
                    return $connection->update($this->escapeIdentifier($table), $this->dataWithFieldAliasMappings($data), $this->dataWithFieldAliasMappings($where));
                });
        }

    }

    /**
     * Taken given field name/value inputs and map them to their aliased names
     */
    public function dataWithFieldAliasMappings(array $data)
    {
        $fields = $this->mapper->entityManager()->fields();
        $fieldMappings = [];
        foreach($data as $field => $value) {
            $fieldAlias = $this->escapeIdentifier(isset($fields[$field]) ? $fields[$field]['column'] : $field);
            $fieldMappings[$fieldAlias] = $value;
        }
        return $fieldMappings;
    }

    /**
     * Taken given aliased field name/value inputs and map them to their non-aliased names
     */
    public function dataWithOutFieldAliasMappings(array $data)
    {
        // have to call fields() otherwise fieldAliasMappings() would return null on the first entity
        $this->mapper->entityManager()->fields();
        $fieldAliasMappings = $this->mapper->entityManager()->fieldAliasMappings();
        foreach ($fieldAliasMappings as $field => $aliasedField) {
            if (array_key_exists($aliasedField, $data)) {
                $data[$field] = $data[$aliasedField];
                unset($data[$aliasedField]);
            }
        }
        return $data;
    }

    /**
     * Execute provided query and return result
     *
     * @param  \lingyiLib\DbMapper\Mapper\Query $query SQL query to execute
     * @return \Doctrine\DBAL\Driver\Statement|int
     * @throws \lingyiLib\DbMapper\Mapper\Exception
     */
    public function exec(Query $query)
    {
        return $query->builder()->execute();
    }

    /**
     * Truncate Table
     *
     * @param string $table Table name
     * @param bool $cascade
     * @return
     * @throws \lingyiLib\DbMapper\Mapper\Exception
     * @internal param array $data Array of data for WHERE clause in 'field' => 'value' format
     */
    public function truncate($table, $cascade = false)
    {
        $mapper = $this->mapper;
        $connection = $mapper->connection();

        $table = $this->escapeIdentifier($table);

        // SQLite doesn't support TRUNCATE
        if ($mapper->connectionIs("sqlite")) {
            $sql = "DELETE FROM " . $table;
        } elseif ($mapper->connectionIs("pgsql")) {
            $sql = "TRUNCATE TABLE " . $table . ($cascade ? " CASCADE" : "");
        } else {
            $sql = "TRUNCATE TABLE " . $table . "";
        }
        try{
            return $connection->transactional(function ($conn) use ($sql) {
                $conn->exec($sql);
            });
        }catch (\Exception $e){
            return $connection->transactional(function ($conn) use ($sql) {
                $conn->exec($sql);
            });
        }
    }

    /**
     * Drop Table
     *
     * @param string $table Table name
     * @return bool
     */
    public function dropTable($table)
    {
        $result = false;
        $connection = $this->mapper->connection();
        try {
            $result = $connection->getSchemaManager()->dropTable($this->escapeIdentifier($table));
        } catch (\Exception $e) {
            $result = false;
        }

        return $result;
    }

    /**
     * Escape/quote identifier
     *
     * @param string $identifier
     * @return string
     */
    public function escapeIdentifier($identifier)
    {
        if($this->_noQuote) {
            return $identifier;
        }

        return $this->mapper->connection()->quoteIdentifier(trim($identifier));
    }

    /**
     * Trim a leading schema name separated by a dot if present
     *
     * @param string $identifier
     * @return string
     */
    public function trimSchemaName($identifier){
            $components = explode('.', $identifier, 2);
            return end($components);
    }

    /**
     * Add foreign keys from BelongsTo relations to the table schema
     * @param Table $table
     * @return Table
     */
    protected function addForeignKeys(Table $table)
    {
        $entityName = $this->mapper->entity();
        $entity = new $entityName;
        $relations = $entityName::relations($this->mapper, $entity);
        $fields = $this->mapper->entityManager()->fields();
        foreach ($relations as $relationName => $relation) {
            if ($relation instanceof BelongsTo) {

                $fieldInfo = $fields[$relation->localKey()];

                if ($fieldInfo['foreignkey'] === false) {
                    continue;
                }

                $foreignTableMapper = $relation->mapper()->getMapper($relation->entityName());
                $foreignTable = $foreignTableMapper->table();

                $foreignSchemaManager = $foreignTableMapper->connection()->getSchemaManager();
                $foreignTableObject = $foreignSchemaManager->listTableDetails($foreignTable);

                $foreignTableColumns = $foreignTableObject->getColumns();
                $foreignTableNotExists = empty($foreignTableColumns);
                $foreignKeyNotExists = !array_key_exists($relation->foreignKey(), $foreignTableColumns);
                // We need to use the is_a() function because the there is some inconsistency in entity names (leading slash)
                $notRecursiveForeignKey = !is_a($entity, $relation->entityName());

                /* Migrate foreign table if:
                 *  - the foreign table not exists
                 *  - the foreign key not exists
                 *  - the foreign table is not the same as the current table (recursion check)
                 * This migration eliminates the 'Integrity constraint violation' error
                 */
                if (($foreignTableNotExists || $foreignKeyNotExists) && $notRecursiveForeignKey){
                    $foreignTableMapper->migrate();
                }

                $onUpdate = !is_null($fieldInfo['onUpdate']) ? $fieldInfo['onUpdate'] :"CASCADE";

                if (!is_null($fieldInfo['onDelete'])) {
                    $onDelete = $fieldInfo['onDelete'];
                } else if ($fieldInfo['notnull']) {
                    $onDelete = "CASCADE";
                } else {
                    $onDelete = "SET NULL";
                }

                // Field alias support
                $fieldAliasMappings = $this->mapper->entityManager()->fieldAliasMappings();
                if (isset($fieldAliasMappings[$relation->localKey()])) {
                    $localKey = $fieldAliasMappings[$relation->localKey()];
                } else {
                    $localKey = $relation->localKey();
                }

                $fkName = $this->mapper->table().'_fk_'.$relationName;
                $table->addForeignKeyConstraint($foreignTable, [$localKey], [$relation->foreignKey()], ["onDelete" => $onDelete, "onUpdate" => $onUpdate], $fkName);
            }
        }

        return $table;
    }

    /**
     * Handle a query exception that occurred during query execution.
     *
     * @throws \Exception
     */
    protected function tryAgainIfCausedByLostConnection(\Exception $e, Closure $callback)
    {
        if ($this->causedByLostConnection($e->getPrevious())) {
            return $this->runCallback($callback);
        }
        throw $e;
    }

    protected function runCallback(Closure $callback)
    {
        try {
            $result = $callback();
        } catch (\Exception $e) {
            throw $e;
        }
        return $result;
    }
}
