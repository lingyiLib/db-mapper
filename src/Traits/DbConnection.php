<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
namespace lingyiLib\DbMapper\Traits;

use Closure;

/**
 * TODO: release connection except transaction.
 */
trait DbConnection
{
    public function beginTransaction(): void
    {
        $this->setTransaction(true);
        $this->__call(__FUNCTION__, func_get_args());
    }

    public function commit(): void
    {
        $this->setTransaction(false);
        $this->__call(__FUNCTION__, func_get_args());
    }

    public function rollBack(): void
    {
        $this->setTransaction(false);
        $this->__call(__FUNCTION__, func_get_args());
    }
}
