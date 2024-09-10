<?php

namespace Arris\Toolkit\SphinxQL;

use Closure;
use Foolz\SphinxQL\Drivers\ConnectionInterface;
use Foolz\SphinxQL\Drivers\ResultSetInterface;
use Foolz\SphinxQL\SphinxQL;
use Psr\Log\LoggerInterface;

interface FoolzWrapperInterface
{
    public static function init(string $sphinx_connection_host, string $sphinx_connection_port, array $options = [], LoggerInterface $logger = null);
    public static function setConsoleMessenger(callable $messenger);

    public static function initConnection(): ConnectionInterface;
    public static function getInstance($connection): SphinxQL;
    public static function createInstance(): SphinxQL;

    public static function getVersion(ConnectionInterface $connection);
    public static function showMeta(ConnectionInterface $connection):array;

    public static function rt_ReplaceIndex(string $index_name, array $dataset = []): ?ResultSetInterface;
    public static function rt_UpdateIndex(string $index_name, array $dataset = []): ?ResultSetInterface;
    public static function rt_DeleteIndex(string $index_name, string $field, $field_value = null): ?ResultSetInterface;
    public static function rt_DeleteIndexMatch(string $index_name, string $field, string $field_value = ''): ?ResultSetInterface;
    public static function rt_TruncateIndex(string $index_name, bool $is_reconfigure = true): bool;

    public static function getDataset(string $search_query, string $source_index, string $sort_field, string $sort_order = 'DESC', int $limit = 5, array $option_weight = []): array;

    public static function rt_RebuildAbstractIndex($pdo_connection, string $sql_source_table, string $sphinx_index, Closure $make_updateset_method, string $condition = ''): int;
}

# -eof- #