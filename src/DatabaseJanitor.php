<?php

namespace DatabaseJanitor;

use Ifsnop\Mysqldump\Mysqldump as MysqlDump;
use Ifsnop\Mysqldump as IMysqldump;

/**
 * Class DatabaseJanitor.
 *
 * @package DatabaseJanitor
 */
class DatabaseJanitor
{

    private $password;
    private $host;
    private $user;
    private $database;
    private $dumpOptions;
    private $connection;
    private $data;

    /**
     * DatabaseJanitor constructor.
     */
    public function __construct($database, $user, $host, $password, $dumpOptions)
    {
        $this->database = $database;
        $this->user = $user;
        $this->host = $host;
        $this->password = $password;
        $this->dumpOptions = $dumpOptions;
        $this->data = [];
        try {
            $this->connection = new \PDO('mysql:host=' . $this->host . ';dbname=' . $this->database, $this->user, $this->password, [
                \PDO::ATTR_PERSISTENT => TRUE,
            ]);
        } catch (\Exception $e) {
            echo $e;
        }
    }

    /**
     * Basic dumping.
     *
     * @return bool|string
     *   FALSE if dump encountered an error, otherwise return location of dump.
     */
    public function dump($host = FALSE, $output = FALSE, $trim = FALSE)
    {
        if (!$output) {
            $output = 'php://stdout';
        }

        if ($host) {
            $this->database = $host->database;
            $this->user = $host->user;
            $this->host = $host->host;
            $this->password = $host->password;
        }

        $dumpSettings = [
            'add-locks' => FALSE,
            'exclude-tables' => $this->dumpOptions['excluded_tables'] ?? [],
            'no-data' => $this->dumpOptions['scrub_tables'] ?? [],
            'keep-data' => $this->dumpOptions['keep_data'] ?? [],
        ];

        try {
            $dump = new MysqlDump('mysql:host=' . $this->host . ';dbname=' . $this->database, $this->user, $this->password, $dumpSettings);

            // Data for skipping rows during sanitization.
            $this->data['row_count'] = 0;
            $this->data['first_col'] = '';
            $this->data['skip_rows'] = 0;

            $dump->setTransformColumnValueHook(function ($table_name, $col_name, $col_value) {
                return $this->skippableSanitize($table_name, $col_name, $col_value, $this->dumpOptions);
            });

            $dump->start($output);
        } catch (\Exception $e) {
            echo 'mysqldump - php error: ' . $e->getMessage();
            return FALSE;
        }
        return $output;
    }

    /**
     * Replace values in specific table col with random value.
     *
     * @param string $table_name
     *   The current table's name.
     * @param string $col_name
     *   The current column name.
     * @param string $col_value
     *   The current value in the column.
     * @param array $options
     *   Full configuration of tables to sanitize.
     *
     * @return string
     *   New col value.
     */
    public function sanitize($table_name, $col_name, $col_value, array $options)
    {
        if (isset($options['sanitize_tables'])) {
            foreach ($options['sanitize_tables'] as $table => $val) {
                if ($table == $table_name) {
                    foreach ($options['sanitize_tables'][$table] as $col) {
                        if ($col == $col_name) {
                            // Generate value based on the type of the actual value.
                            // Helps avoid breakage with incorrect types in cols.
                            switch (gettype($col_value)) {
                                case 'integer':
                                case 'double':
                                    return random_int(1000000, 9999999);

                                case 'string':
                                    return (string)random_int(1000000, 9999999) . '-janitor';

                                default:
                                    return $col_value;
                            }
                        }
                    }
                }
            }
        }

        return $col_value;
    }

    /**
     * Replace values in specific table col with random value allowing the ability to skip rows.
     *
     * @param string $table_name
     *   The current table's name.
     * @param string $col_name
     *   The current column name.
     * @param string $col_value
     *   The current value in the column.
     * @param array $options
     *   Full configuration of tables to sanitize.
     *
     * @return string
     *   New col value.
     */
    public function skippableSanitize($table_name, $col_name, $col_value, array $options)
    {
        $has_skip_logic = FALSE;

        // Check if current table has rows to skip sanitization.
        // To be replaced with setTableLimits() when released.
        if (isset($options['sanitize_table_rows_exclude'][$table_name])) {
            $this->data['first_col'] = $options['sanitize_table_rows_exclude'][$table_name]['first_col'];
            $this->data['skip_rows'] = $options['sanitize_table_rows_exclude'][$table_name]['skip_rows'];
            $has_skip_logic = TRUE;
        } else {
            $this->data['row_count'] = 0;
        }

        // Increase Row count.
        if ($col_name == $this->data['first_col']) {
            $this->data['row_count']++;
        }

        // Check if we passed the row count for skippable rows.
        if (!$has_skip_logic || $this->data['row_count'] > $this->data['skip_rows']) {
            return $this->sanitize($table_name, $col_name, $col_value, $options);
        } else {
            return $col_value;
        }
    }

}
