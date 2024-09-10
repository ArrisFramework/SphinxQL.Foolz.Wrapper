<?php

namespace Arris\Toolkit\SphinxQL;

use Closure;
use Exception;
use Foolz\SphinxQL\Drivers\ConnectionInterface;
use Foolz\SphinxQL\Drivers\Mysqli\Connection;
use Foolz\SphinxQL\Drivers\ResultSetInterface;
use Foolz\SphinxQL\Exception\ConnectionException;
use Foolz\SphinxQL\Exception\DatabaseException;
use Foolz\SphinxQL\Exception\SphinxQLException;
use Foolz\SphinxQL\Helper;
use Foolz\SphinxQL\SphinxQL;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 *
 */
class FoolzWrapper implements FoolzWrapperInterface
{
    /**
     * @var Connection|string
     */
    private static $host;

    /**
     * @var int|string
     */
    private static $port;

    /**
     * rebuild_logging_options
     *
     * @var array
     */
    private static array $options = [];

    /**
     * @var ConnectionInterface
     */
    private static $connection;

    /**
     * @var SphinxQL
     */
    private static $instance;

    /**
     * @var LoggerInterface
     */
    private static $logger;

    private static \Closure $messenger;

    /**
     * Инициализирует статический интерфейс к методам
     *
     * @param string $sphinx_connection_host
     * @param string $sphinx_connection_port
     * @param array $options
     * @param LoggerInterface|null $logger
     */
    public static function init(string $sphinx_connection_host, string $sphinx_connection_port, array $options = [], LoggerInterface $logger = null)
    {
        self::$host = $sphinx_connection_host;
        self::$port = (int)$sphinx_connection_port;

        self::$options['chunk_length']          = self::setOption($options, 'chunk_length', 500);

        self::$options['log_rows_inside_chunk'] = self::setOption($options, 'log_rows_inside_chunk', true);
        self::$options['log_total_rows_found']  = self::setOption($options, 'log_total_rows_found', true);

        self::$options['log_before_chunk']      = self::setOption($options, 'log_before_chunk', true);
        self::$options['log_after_chunk']       = self::setOption($options, 'log_after_chunk', true);

        self::$options['sleep_after_chunk']     = self::setOption($options, 'sleep_after_chunk', true);

        self::$options['sleep_time'] = self::setOption($options, 'sleep_time', 1);
        if (self::$options['sleep_time'] == 0) {
            self::$options['sleep_after_chunk'] = false;
        }

        self::$options['log_before_index']      = self::setOption($options, 'log_before_index', true);
        self::$options['log_after_index']       = self::setOption($options, 'log_after_index', true);

        self::$logger = \is_null($logger) ? new NullLogger() : $logger;

        self::$messenger = function($message = '', $linebreak = true) {
            echo $message;
            if ($linebreak) {
                echo PHP_EOL;
            }
        };
    }

    /**
     * Устанавливает консольный мессенджер
     *
     * @param callable $messenger
     * @return void
     */
    public static function setConsoleMessenger(callable $messenger)
    {
        self::$messenger = $messenger;
    }

    /**
     * Создает коннекшен и устанавливает параметры подключения: хост и порт
     *
     * @return ConnectionInterface
     */
    public static function initConnection(): ConnectionInterface
    {
        $connection = new Connection();
        $connection->setParams([
            'host' => self::$host,
            'port' => self::$port
        ]);

        return $connection;
    }

    /**
     * Создает инстанс на основе сохраненного в классе коннекшена
     *
     * @param ConnectionInterface $connection
     * @return SphinxQL
     */
    public static function getInstance($connection): SphinxQL
    {
        return (new SphinxQL($connection));
    }

    /**
     * Создает инстанс SphinxQL (для однократного обновления)
     *
     * @return SphinxQL
     */
    public static function createInstance(): SphinxQL
    {
        self::$connection = self::initConnection();
        self::$instance = self::getInstance(self::$connection);

        return self::$instance;
    }

    /**
     * Замещает (REPLACE) реалтайм-индекс по набору данных
     *
     * @param string $index_name
     * @param array $dataset
     * @return ResultSetInterface|null
     *
     * @throws DatabaseException
     * @throws ConnectionException
     * @throws SphinxQLException
     */
    public static function rt_ReplaceIndex(string $index_name, array $dataset = []): ?ResultSetInterface
    {
        if (empty($dataset)) {
            return null;
        }

        return self::createInstance()
            ->replace()
            ->into($index_name)
            ->set($dataset)
            ->execute();
    }

    /**
     * Обновляет (UPDATE) реалтайм-индекс по набору данных
     * с созданием коннекшена "сейчас"
     *
     * @param string $index_name
     * @param array $dataset
     * @return ResultSetInterface|null
     *
     * @throws DatabaseException
     * @throws ConnectionException
     * @throws SphinxQLException
     */
    public static function rt_UpdateIndex(string $index_name, array $dataset = []): ?ResultSetInterface
    {
        if (empty($dataset)) {
            return null;
        }

        return self::createInstance()
            ->update($index_name)
            ->into($index_name)
            ->set($dataset)
            ->execute();
    }

    /**
     * Удаляет строку реалтайм-индекса по значению нестрокового поля.
     *
     * @todo: при передаче параметра требуется его приведение к типу поля. Для поля 'id' это тип INT.
     *
     * В случае multi-valued атрибута нужно удалять строки для каждого значения атрибута.
     *
     * @param string $index_name        -- индекс
     * @param string $field             -- поле для поиска индекса
     * @param null $field_value         -- значение для поиска индекса
     * @return ResultSetInterface|null
     *
     * @throws DatabaseException
     * @throws ConnectionException
     * @throws SphinxQLException
     */
    public static function rt_DeleteIndex(string $index_name, string $field, $field_value = null): ?ResultSetInterface
    {
        if (is_null($field_value)) {
            return null;
        }

        return self::createInstance()
            ->delete()
            ->from($index_name)
            ->where($field, '=', $field_value)
            ->execute();
    }

    /**
     * Удаляет строку реалтайм-индекса по значению текстового поля, например '@title поликлиника'
     * ВАЖНО: пустое значение поля $field_value удалит ВСЕ строки индекса
     *
     * @param string $index_name        -- индекс
     * @param string $field             -- поле для поиска индекса
     * @param string $field_value       -- значение для поиска индекса (важно: тип значения должен совпадать)
     * @return ResultSetInterface|null
     *
     * @throws DatabaseException
     * @throws ConnectionException
     * @throws SphinxQLException
     */
    public static function rt_DeleteIndexMatch(string $index_name, string $field, string $field_value = ''): ?ResultSetInterface
    {
        if (is_null($field_value)) {
            return null;
        }

        return self::createInstance()
            ->delete()
            ->from($index_name)
            ->match($field, $field_value)
            ->execute();
    }

    /**
     * Делает truncate index с реконфигурацией по умолчанию
     *
     * @param string $index_name
     * @param bool $is_reconfigure
     * @return bool
     */
    public static function rt_TruncateIndex(string $index_name, bool $is_reconfigure = true): bool
    {
        if (empty($index_name)) {
            return false;
        }

        $with = $is_reconfigure ? 'WITH RECONFIGURE' : '';

        return (bool)self::createInstance()->query("TRUNCATE RTINDEX {$index_name} {$with}");
    }


    /**
     *
     * @param ConnectionInterface $connection
     * @return array
     *
     * @throws \Foolz\SphinxQL\Exception\ConnectionException
     * @throws \Foolz\SphinxQL\Exception\DatabaseException
     * @throws \Foolz\SphinxQL\Exception\SphinxQLException
     */
    public static function showMeta(ConnectionInterface $connection):array
    {
        return (new Helper($connection))->showMeta()->execute()->fetchAllAssoc();
    }

    /**
     * Возвращает версию поискового движка
     *
     * @param ConnectionInterface $connection
     * @throws \Foolz\SphinxQL\Exception\ConnectionException
     * @throws \Foolz\SphinxQL\Exception\DatabaseException
     */
    public static function getVersion(ConnectionInterface $connection)
    {
        $connection->query("SHOW STATUS LIKE 'version%'")->fetchAssoc()['version'];
    }

    /**
     * Unused?
     *
     *
     * @param string $search_query
     * @param string $source_index
     * @param string $sort_field
     * @param string $sort_order
     * @param int $limit
     * @param array $option_weight
     * @return array
     *
     * @throws ConnectionException
     * @throws DatabaseException
     * @throws SphinxQLException
     */
    public static function getDataset(string $search_query, string $source_index, string $sort_field, string $sort_order = 'DESC', int $limit = 5, array $option_weight = []): array
    {
        $found_dataset = [];
        $compiled_request = '';

        if (empty($source_index)) {
            return $found_dataset;
        }

        try {
            $search_request = self::createInstance()
                ->select()
                ->from($source_index);

            if (!empty($sort_field)) {
                $search_request = $search_request
                    ->orderBy($sort_field, $sort_order);
            }

            if (!empty($option_weight)) {
                $search_request = $search_request
                    ->option('field_weights', $option_weight);
            }

            if (!\is_null($limit) && \is_numeric($limit)) {
                $search_request = $search_request
                    ->limit($limit);
            }

            if (\strlen($search_query) > 0) {
                $search_request = $search_request
                    ->match(['title'], $search_query);
            }

            $compiled_request = $search_request->getCompiled();

            $search_result = $search_request->execute();

            while ($row = $search_result->fetchAssoc()) {
                $found_dataset[] = $row['id'];
            }

        } catch (Exception $e) {

            $meta = self::showMeta(self::$connection);

            self::$logger->error(
                __CLASS__ . '/' . __METHOD__ .
                " Error fetching data from [{$source_index}] : " . $e->getMessage(),
                [
                    $e->getCode(),
                    \htmlspecialchars(\urldecode($_SERVER['HTTP_HOST'] . $_SERVER['REQUEST_URI'])),
                    $compiled_request,
                    $meta
                ]
            );
        }
        return $found_dataset;
    } // get_IDs_DataSet()

    /**
     * Unused
     *
     * @param $pdo_connection
     * @param string $sql_source_table
     * @param string $sphinx_index
     * @param Closure $make_updateset_method
     * @param string $condition
     * @return int
     * @throws DatabaseException
     * @throws ConnectionException
     * @throws SphinxQLException
     */
    public static function rt_RebuildAbstractIndex($pdo_connection, string $sql_source_table, string $sphinx_index, Closure $make_updateset_method, string $condition = ''): int
    {
        $chunk_size = self::$options['chunk_length'];

        self::rt_TruncateIndex($sphinx_index);

        $total_count
            = $pdo_connection
            ->query("SELECT COUNT(*) as cnt FROM {$sql_source_table} " . ($condition != '' ? " WHERE {$condition} " : ' ') )
            ->fetchColumn();

        $total_updated = 0;

        if (self::$options['log_before_index']) {
            call_user_func_array(self::$messenger, [
                "<font color='yellow'>[{$sphinx_index}]</font> index : ",
                false
            ]);
        }

        if (self::$options['log_total_rows_found']) {
            call_user_func_array(self::$messenger, [
                "<font color='green'>{$total_count}</font> elements found for rebuild."
            ]);
        }

        // iterate chunks
        for ($i = 0; $i < ceil($total_count / $chunk_size); $i++) {
            $offset = $i * $chunk_size;

            if (self::$options['log_before_chunk']) {
                call_user_func_array(self::$messenger, [
                    "Rebuilding elements from <font color='green'>{$offset}</font>, <font color='yellow'>{$chunk_size}</font> count... ",
                    false
                ]);
            }

            $query_chunk_data = "SELECT * FROM {$sql_source_table} ";
            $query_chunk_data.= $condition != '' ? " WHERE {$condition} " : ' ';
            $query_chunk_data.= "ORDER BY id DESC LIMIT {$offset}, {$chunk_size} ";

            $sth = $pdo_connection->query($query_chunk_data);

            // iterate inside chunk
            while ($item = $sth->fetch()) {
                if (self::$options['log_rows_inside_chunk']) {
                    call_user_func_array(self::$messenger, [
                        "{$sql_source_table}: {$item['id']}"
                    ]);
                }

                // $update_set = $make_updateset_method($item); // call closure
                $update_set = call_user_func($make_updateset_method,  $item );

                self::replaceIndexRow($sphinx_index, $update_set);

                $total_updated++;
            } // while

            $breakline_after_chunk = !self::$options['sleep_after_chunk'];

            if (self::$options['log_after_chunk']) {
                call_user_func_array(self::$messenger, [
                    "Updated RT-index <font color='yellow'>{$sphinx_index}</font>.",
                    $breakline_after_chunk
                ]);
            } else {
                call_user_func_array(self::$messenger, [
                    "<strong>Ok</strong>",
                    $breakline_after_chunk
                ]);
            }

            if (self::$options['sleep_after_chunk']) {
                call_user_func_array(self::$messenger, [
                    "  ZZZZzzz for " . self::$options['sleep_time'] . " second(s)... ",
                    false
                ]);
                sleep(self::$options['sleep_time']);
                call_user_func_array(self::$messenger, [
                    "I woke up!"
                ]);
            }
        } // for

        if (self::$options['log_after_index']) {
            call_user_func_array(self::$messenger, [
                "Total updated <strong>{$total_updated}</strong> elements for <font color='yellow'>{$sphinx_index}</font> RT-index. <br>"
            ]);
        }

        return $total_updated;
    }


    /**
     * Заменяет одну строчку в индексе. Никаких prepared statements.
     *
     * @param string $index
     * @param array $dataset
     * @return ResultSetInterface|null
     * @throws DatabaseException
     * @throws ConnectionException
     * @throws SphinxQLException
     */
    private static function replaceIndexRow(string $index = '', array $dataset = []): ?ResultSetInterface
    {
        if (empty($index)) {
            return null;
        }
        if (empty($dataset)) {
            return null;
        }

        return self::getInstance(self::$connection)
            ->replace()
            ->into($index)
            ->set($dataset)
            ->execute();
    }

    /**
     *
     * @param array $options
     * @param null $key
     * @param null $default_value
     * @return mixed|null
     */
    protected static function setOption(array $options = [], $key = null, $default_value = null)
    {
        if (!\is_array($options)) {
            return $default_value;
        }

        if (\is_null($key)) {
            return $default_value;
        }

        return \array_key_exists($key, $options) ? $options[ $key ] : $default_value;
    }

}

# -eof- #