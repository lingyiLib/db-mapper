<?php

namespace lingyiLib\DbMapper\Mapper\Query\Operator;

use Doctrine\DBAL\Query\QueryBuilder;

/**
 * @package lingyiLib\DbMapper\Mapper\Query\Operator
 */
class GreaterThan
{
    /**
     * @param QueryBuilder $builder
     * @param $column
     * @param $value
     * @return string
     */
    public function __invoke(QueryBuilder $builder, $column, $value)
    {
        return $column . ' > ' . $builder->createPositionalParameter($value);
    }
}
