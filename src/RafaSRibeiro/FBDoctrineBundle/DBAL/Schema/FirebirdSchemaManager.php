<?php

/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace RafaSRibeiro\FBDoctrineBundle\DBAL\Schema;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Types\StringType;
use Doctrine\DBAL\Types\TextType;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Events;

/**
 * Sqlite SchemaManager.
 *
 * @author Konsta Vesterinen <kvesteri@cc.hut.fi>
 * @author Lukas Smith <smith@pooteeweet.org> (PEAR MDB2 library)
 * @author Jonathan H. Wage <jonwage@gmail.com>
 * @author Martin Hasoň <martin.hason@gmail.com>
 * @since  2.0
 */
class FirebirdSchemaManager extends AbstractSchemaManager {

    /**
     * {@inheritdoc}
     */
    public function dropDatabase($database) {
        if (file_exists($database)) {
            unlink($database);
        }
    }

    /**
     * Aggregates and groups the index results according to the required data result.
     *
     * @param array       $tableIndexRows
     * @param string|null $tableName
     *
     * @return array
     */
    protected function _getPortableTableIndexesList($tableIndexRows, $tableName = null) {
        $result = array();
        foreach ($tableIndexRows as $tableIndex) {
            $indexName = $keyName = $tableIndex['CONSTRAINT_NAME'];
            if ($tableIndex['TYPE_CONTRAINT'] == 'PK') {
                $keyName = 'primary';
            }
            $keyName = strtolower($keyName);

            if (!isset($result[$keyName])) {
                $result[$keyName] = array(
                    'name' => $indexName,
                    'columns' => array($tableIndex['FIELD_NAME']),
                    'unique' => $tableIndex['UNIQUE_FIELD'] == '1' ? false : true,
                    'primary' => $tableIndex['TYPE_CONTRAINT']
//                    'flags' => isset($tableIndex['flags']) ? $tableIndex['flags'] : array(),
//                    'options' => isset($tableIndex['where']) ? array('where' => $tableIndex['where']) : array(),
                );
            } else {
                $result[$keyName]['columns'][] = $tableIndex['FIELD_NAME'];
            }
        }

        $eventManager = $this->_platform->getEventManager();

        $indexes = array();
        foreach ($result as $indexKey => $data) {
            $index = null;
            $defaultPrevented = false;

            if (null !== $eventManager && $eventManager->hasListeners(Events::onSchemaIndexDefinition)) {
                $eventArgs = new SchemaIndexDefinitionEventArgs($data, $tableName, $this->_conn);
                $eventManager->dispatchEvent(Events::onSchemaIndexDefinition, $eventArgs);

                $defaultPrevented = $eventArgs->isDefaultPrevented();
                $index = $eventArgs->getIndex();
            }

            if (!$defaultPrevented) {
                $index = new Index($data['name'], $data['columns'], $data['unique'], $data['primary']);
            }

            if ($index) {
                $indexes[$indexKey] = $index;
            }
        }

        return $indexes;
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableColumnDefinition($tableColumn) {
        $parts = explode('(', $tableColumn['FIELD_TYPE']);
        $tableColumn['FIELD_TYPE'] = $parts[0];
        if (isset($parts[1])) {
            $length = trim($parts[1], ')');
            $tableColumn['FIELD_LENGTH'] = $length;
        }

        $dbType = strtolower(trim($tableColumn['FIELD_TYPE']));
        $length = isset($tableColumn['FIELD_LENGTH']) ? $tableColumn['FIELD_LENGTH'] : null;
        $unsigned = false;

        if (strpos($dbType, ' unsigned') !== false) {
            $dbType = str_replace(' unsigned', '', $dbType);
            $unsigned = true;
        }

        $fixed = false;
        $type = $this->_platform->getDoctrineTypeMapping($dbType);
        $default = $tableColumn['DEFAULT_VALUE'];
        if ($default == 'NULL') {
            $default = null;
        }
        if ($default !== null) {
            $default = preg_replace("/^'(.*)'$/", '\1', $default);
        }
        $notnull = (bool) $tableColumn['NOT_NULL'];

        if (!isset($tableColumn['FIELD_NAME'])) {
            $tableColumn['FIELD_NAME'] = '';
        }

        $precision = null;
        $scale = null;

        switch ($dbType) {
            case 'char':
                $fixed = true;
                break;
            case 'float':
            case 'double':
            case 'real':
            case 'decimal':
            case 'numeric':
                if (isset($tableColumn['FIELD_LENGTH'])) {
                    if (strpos($tableColumn['FIELD_LENGTH'], ',') === false) {
                        $tableColumn['FIELD_LENGTH'] .= ",0";
                    }
                    list($precision, $scale) = array_map('trim', explode(',', $tableColumn['FIELD_LENGTH']));
                }
                $length = null;
                break;
        }

        $options = array(
            'length' => $length,
            'unsigned' => (bool) $unsigned,
            'fixed' => $fixed,
            'notnull' => $notnull,
            'default' => $default,
            'precision' => $precision,
            'scale' => $scale,
            'autoincrement' => false,
        );

        return new Column($tableColumn['FIELD_NAME'], \Doctrine\DBAL\Types\Type::getType($type), $options);
    }

    /**
     * @param string $tableName
     *
     * @return \Doctrine\DBAL\Schema\Table
     */
    public function listTableDetails($tableName) {
        $columns = $this->listTableColumns($tableName['TABLE_NAME']);
        $foreignKeys = array();
        if ($this->_platform->supportsForeignKeyConstraints()) {
            $foreignKeys = $this->listTableForeignKeys($tableName['TABLE_NAME']);
        }
        $indexes = $this->listTableIndexes($tableName['TABLE_NAME']);

        return new Table($tableName['TABLE_NAME'], $columns, $indexes, $foreignKeys, false, array());
    }

    /**
     * {@inheritdoc}
     */
    public function createDatabase($database) {
        $params = $this->_conn->getParams();
        $driver = $params['driver'];
        $options = array(
            'driver' => $driver,
            'path' => $database
        );
        $conn = \Doctrine\DBAL\DriverManager::getConnection($options);
        $conn->connect();
        $conn->close();
    }

    /**
     * {@inheritdoc}
     */
    public function renameTable($name, $newName) {
        $tableDiff = new TableDiff($name);
        $tableDiff->fromTable = $this->listTableDetails($name);
        $tableDiff->newName = $newName;
        $this->alterTable($tableDiff);
    }

    /**
     * {@inheritdoc}
     */
    public function createForeignKey(ForeignKeyConstraint $foreignKey, $table) {
        $tableDiff = $this->getTableDiffForAlterForeignKey($foreignKey, $table);
        $tableDiff->addedForeignKeys[] = $foreignKey;

        $this->alterTable($tableDiff);
    }

    /**
     * {@inheritdoc}
     */
    public function dropAndCreateForeignKey(ForeignKeyConstraint $foreignKey, $table) {
        $tableDiff = $this->getTableDiffForAlterForeignKey($foreignKey, $table);
        $tableDiff->changedForeignKeys[] = $foreignKey;

        $this->alterTable($tableDiff);
    }

    /**
     * {@inheritdoc}
     */
    public function dropForeignKey($foreignKey, $table) {
        $tableDiff = $this->getTableDiffForAlterForeignKey($foreignKey, $table);
        $tableDiff->removedForeignKeys[] = $foreignKey;

        $this->alterTable($tableDiff);
    }

    /**
     * {@inheritdoc}
     */
    public function listTableForeignKeys($table, $database = null) {
        if (null === $database) {
            $database = $this->_conn->getDatabase();
        }
        $sql = $this->_platform->getListTableForeignKeysSQL($table, $database);
        $tableForeignKeys = $this->_conn->fetchAll($sql);

        if (!empty($tableForeignKeys)) {
            $createSql = $this->_conn->fetchAll("SELECT sql FROM (SELECT * FROM sqlite_master UNION ALL SELECT * FROM sqlite_temp_master) WHERE type = 'table' AND name = '$table'");
            $createSql = isset($createSql[0]['sql']) ? $createSql[0]['sql'] : '';
            if (preg_match_all('#
                    (?:CONSTRAINT\s+([^\s]+)\s+)?
                    (?:FOREIGN\s+KEY[^\)]+\)\s*)?
                    REFERENCES\s+[^\s]+\s+(?:\([^\)]+\))?
                    (?:
                        [^,]*?
                        (NOT\s+DEFERRABLE|DEFERRABLE)
                        (?:\s+INITIALLY\s+(DEFERRED|IMMEDIATE))?
                    )?#isx', $createSql, $match)) {

                $names = array_reverse($match[1]);
                $deferrable = array_reverse($match[2]);
                $deferred = array_reverse($match[3]);
            } else {
                $names = $deferrable = $deferred = array();
            }

            foreach ($tableForeignKeys as $key => $value) {
                $id = $value['id'];
                $tableForeignKeys[$key]['constraint_name'] = isset($names[$id]) && '' != $names[$id] ? $names[$id] : $id;
                $tableForeignKeys[$key]['deferrable'] = isset($deferrable[$id]) && 'deferrable' == strtolower($deferrable[$id]) ? true : false;
                $tableForeignKeys[$key]['deferred'] = isset($deferred[$id]) && 'deferred' == strtolower($deferred[$id]) ? true : false;
            }
        }

        return $this->_getPortableTableForeignKeysList($tableForeignKeys);
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableIndexDefinition($tableIndex) {
        return array(
            'name' => $tableIndex['name'],
            'unique' => (bool) $tableIndex['unique']
        );
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableColumnList($table, $database, $tableColumns) {
        $eventManager = $this->_platform->getEventManager();
        $list = array();

        foreach ($tableColumns as $tableColumn) {
            $column = null;
            $defaultPrevented = false;
            if (null !== $eventManager && $eventManager->hasListeners(Events::onSchemaColumnDefinition)) {
                $eventArgs = new cls($tableColumn, $table, $database, $this->_conn);
                $eventManager->dispatchEvent(Events::onSchemaColumnDefinition, $eventArgs);

                $defaultPrevented = $eventArgs->isDefaultPrevented();
                $column = $eventArgs->getColumn();
            }

            if (!$defaultPrevented) {
                $column = $this->_getPortableTableColumnDefinition($tableColumn);
            }

            if ($column) {
                $name = strtolower($column->getQuotedName($this->_platform));
                $list[$name] = $column;
            }
        }

        // find column with autoincrement
        $autoincrementColumn = null;
        $autoincrementCount = 0;
        foreach ($tableColumns as $tableColumn) {
            if ('0' != $tableColumn['PK']) {
                $autoincrementCount++;
                if (null === $autoincrementColumn && 'integer' == strtolower($tableColumn['FIELD_TYPE'])) {
                    $autoincrementColumn = $tableColumn['FIELD_NAME'];
                }
            }
        }

        if (1 == $autoincrementCount && null !== $autoincrementColumn) {
            foreach ($list as $column) {
                if ($autoincrementColumn == $column->getName()) {
                    $column->setAutoincrement(true);
                }
            }
        }
        // inspect column collation
        $createSql = $this->getSqlCreateTable($table, $tableColumns);
        foreach ($list as $columnName => $column) {
            $type = $column->getType();

            if ($type instanceof StringType || $type instanceof TextType) {
                $column->setPlatformOption('collation', $this->parseColumnCollationFromSQL($columnName, $createSql) ? : 'BINARY');
            }
        }

        return $list;
    }

    private function getSqlCreateTable($table, $tableColumns) {
        $sqlCreateTable = ' CREATE TABLE ' . $table . ' ( ';
        $fieldsSQL = array();
        foreach ($tableColumns as $field) {
            if ($field['FIELD_TYPE'] === 'varchar') {
                $fieldType = $field['FIELD_TYPE'] . '(' . $field['FIELD_LENGTH'] . ')';
            } else {
                $fieldType = $field['FIELD_TYPE'];
            }

            $default = null;
            if ($field['DEFAULT_VALUE']) {
                $default = ' DEFAULT ' . $field['DEFAULT_VALUE'];
            }

            $notNull = null;
            if ($field['NOT_NULL'] == 1) {
                $notNull = 'NOT NULL';
            }
            $fieldsSQL[] = ' ' . $field['FIELD_NAME'] . ' ' . $fieldType . ' ' . $default . ' ' . $notNull;
        }
        $sqlCreateTable .= implode(',', $fieldsSQL);
        $sqlCreateTable .= ');';
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableViewDefinition($view) {
        return new View($view['name'], $view['sql']);
    }

    /**
     * {@inheritdoc}
     */
    protected function _getPortableTableForeignKeysList($tableForeignKeys) {
        $list = array();
        foreach ($tableForeignKeys as $value) {
            $value = array_change_key_case($value, CASE_LOWER);
            $name = $value['constraint_name'];
            if (!isset($list[$name])) {
                if (!isset($value['on_delete']) || $value['on_delete'] == "RESTRICT") {
                    $value['on_delete'] = null;
                }
                if (!isset($value['on_update']) || $value['on_update'] == "RESTRICT") {
                    $value['on_update'] = null;
                }

                $list[$name] = array(
                    'name' => $name,
                    'local' => array(),
                    'foreign' => array(),
                    'foreignTable' => $value['table'],
                    'onDelete' => $value['on_delete'],
                    'onUpdate' => $value['on_update'],
                    'deferrable' => $value['deferrable'],
                    'deferred' => $value['deferred'],
                );
            }
            $list[$name]['local'][] = $value['from'];
            $list[$name]['foreign'][] = $value['to'];
        }

        $result = array();
        foreach ($list as $constraint) {
            $result[] = new ForeignKeyConstraint(
                    array_values($constraint['local']), $constraint['foreignTable'], array_values($constraint['foreign']), $constraint['name'], array(
                'onDelete' => $constraint['onDelete'],
                'onUpdate' => $constraint['onUpdate'],
                'deferrable' => $constraint['deferrable'],
                'deferred' => $constraint['deferred'],
                    )
            );
        }

        return $result;
    }

    /**
     * @param \Doctrine\DBAL\Schema\ForeignKeyConstraint $foreignKey
     * @param \Doctrine\DBAL\Schema\Table|string         $table
     *
     * @return \Doctrine\DBAL\Schema\TableDiff
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    private function getTableDiffForAlterForeignKey(ForeignKeyConstraint $foreignKey, $table) {
        if (!$table instanceof Table) {
            $tableDetails = $this->tryMethod('listTableDetails', $table);
            if (false === $table) {
                throw new DBALException(sprintf('Sqlite schema manager requires to modify foreign keys table definition "%s".', $table));
            }

            $table = $tableDetails;
        }

        $tableDiff = new TableDiff($table->getName());
        $tableDiff->fromTable = $table;

        return $tableDiff;
    }

    private function parseColumnCollationFromSQL($column, $sql) {
        if (preg_match(
                        '{(?:' . preg_quote($column) . '|' . preg_quote($this->_platform->quoteSingleIdentifier($column)) . ')
                [^,(]+(?:\([^()]+\)[^,]*)?
                (?:(?:DEFAULT|CHECK)\s*(?:\(.*?\))?[^,]*)*
                COLLATE\s+["\']?([^\s,"\')]+)}isx', $sql, $match)) {
            return $match[1];
        }

        return false;
    }

}
