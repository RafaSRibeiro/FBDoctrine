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

namespace RafaSRibeiro\FBDoctrineBundle\DBAL\Platforms;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Identifier;
use Doctrine\DBAL\Schema\Constraint;

/**
 * The FirebirdPlatform class describes the specifics and dialects of the Firebird
 * database platform.
 *
 * @since  2.0
 * @author Roman Borschel <roman@code-factory.org>
 * @author Benjamin Eberlei <kontakt@beberlei.de>
 * @author Martin Hasoň <martin.hason@gmail.com>
 * @todo   Rename: FirebirdPlatform
 */
class FirebirdPlatform extends AbstractPlatform {

    /**
     * {@inheritDoc}
     */
    public function getRegexpExpression() {
        return 'REGEXP';
    }

    /**
     * {@inheritDoc}
     */
    public function getGuidExpression() {
        return "HEX(RANDOMBLOB(4)) || '-' || HEX(RANDOMBLOB(2)) || '-4' || "
                . "SUBSTR(HEX(RANDOMBLOB(2)), 2) || '-' || "
                . "SUBSTR('89AB', 1 + (ABS(RANDOM()) % 4), 1) || "
                . "SUBSTR(HEX(RANDOMBLOB(2)), 2) || '-' || HEX(RANDOMBLOB(6))";
    }

    /**
     * {@inheritDoc}
     */
    public function getSequenceNextValSQL($sequenceName) {
        return 'SELECT GEN_ID(' . $sequenceName . ', 1) from RDB$DATABASE';
    }

    /**
     * {@inheritDoc}
     */
    public function getNowExpression($type = 'timestamp') {
        switch ($type) {
            case 'time':
                return 'current_time';
            case 'date':
                return 'current_date';
            case 'timestamp':
            default:
                return 'current_timestamp';
        }
    }

    /**
     * {@inheritDoc}
     */
    public function getTrimExpression($str, $pos = self::TRIM_UNSPECIFIED, $char = false) {
        $trimChar = ($char != false) ? (', ' . $char) : '';

        switch ($pos) {
            case self::TRIM_LEADING:
                $trimFn = 'LTRIM';
                break;

            case self::TRIM_TRAILING:
                $trimFn = 'RTRIM';
                break;

            default:
                $trimFn = 'TRIM';
        }

        return $trimFn . '(' . $str . $trimChar . ')';
    }

    /**
     * {@inheritDoc}
     *
     * SQLite only supports the 2 parameter variant of this function
     */
    public function getSubstringExpression($value, $position, $length = null) {
        if ($length !== null) {
            return 'substring(' . $value . ' FROM ' . $position . ' FOR ' . $length . ')';
        }

        return 'substring(' . $value . ' FROM ' . $position . ' FROM CHAR_LENGTH(' . $value . '))';
    }

    /**
     * {@inheritDoc}
     */
    public function getLocateExpression($str, $substr, $startPos = false) {
        if ($startPos == false) {
            return 'position(' . $substr . ', ' . $str . ')';
        }

        return 'position(' . $substr . ', ' . $str . ', ' . $startPos . ')';
    }

    /**
     * {@inheritdoc}
     */
    protected function getDateArithmeticIntervalExpression($date, $operator, $interval, $unit) {
        switch ($unit) {
            case self::DATE_INTERVAL_UNIT_SECOND:
            case self::DATE_INTERVAL_UNIT_MINUTE:
            case self::DATE_INTERVAL_UNIT_HOUR:
                return "DATETIME(" . $date . ",'" . $operator . $interval . " " . $unit . "')";

            default:
                switch ($unit) {
                    case self::DATE_INTERVAL_UNIT_WEEK:
                        $interval *= 7;
                        $unit = self::DATE_INTERVAL_UNIT_DAY;
                        break;

                    case self::DATE_INTERVAL_UNIT_QUARTER:
                        $interval *= 3;
                        $unit = self::DATE_INTERVAL_UNIT_MONTH;
                        break;
                }

                return "DATE(" . $date . ",'" . $operator . $interval . " " . $unit . "')";
        }
    }

    /**
     * {@inheritDoc}
     */
    public function getDateDiffExpression($date1, $date2) {
        return 'ROUND(JULIANDAY(' . $date1 . ')-JULIANDAY(' . $date2 . '))';
    }

    /**
     * {@inheritDoc}
     */
    protected function _getTransactionIsolationLevelSQL($level) {
        switch ($level) {
            case \Doctrine\DBAL\Connection::TRANSACTION_READ_UNCOMMITTED:
                return 0;
            case \Doctrine\DBAL\Connection::TRANSACTION_READ_COMMITTED:
            case \Doctrine\DBAL\Connection::TRANSACTION_REPEATABLE_READ:
            case \Doctrine\DBAL\Connection::TRANSACTION_SERIALIZABLE:
                return 1;
            default:
                return parent::_getTransactionIsolationLevelSQL($level);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function getSetTransactionIsolationSQL($level) {
        return 'PRAGMA read_uncommitted = ' . $this->_getTransactionIsolationLevelSQL($level);
    }

    /**
     * {@inheritDoc}
     */
    public function prefersIdentityColumns() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function getBooleanTypeDeclarationSQL(array $field) {
        return 'BOOLEAN';
    }

    /**
     * {@inheritDoc}
     */
    public function getIntegerTypeDeclarationSQL(array $field) {
        return 'INTEGER' . $this->_getCommonIntegerTypeDeclarationSQL($field);
    }

    /**
     * {@inheritDoc}
     */
    public function getBigIntTypeDeclarationSQL(array $field) {
        //  SQLite autoincrement is implicit for INTEGER PKs, but not for BIGINT fields.
        if (!empty($field['autoincrement'])) {
            return $this->getIntegerTypeDeclarationSQL($field);
        }

        return 'BIGINT' . $this->_getCommonIntegerTypeDeclarationSQL($field);
    }

    /**
     * {@inheritDoc}
     */
    public function getTinyIntTypeDeclarationSql(array $field) {
        //  SQLite autoincrement is implicit for INTEGER PKs, but not for TINYINT fields.
        if (!empty($field['autoincrement'])) {
            return $this->getIntegerTypeDeclarationSQL($field);
        }

        return 'TINYINT' . $this->_getCommonIntegerTypeDeclarationSQL($field);
    }

    /**
     * {@inheritDoc}
     */
    public function getSmallIntTypeDeclarationSQL(array $field) {
        //  SQLite autoincrement is implicit for INTEGER PKs, but not for SMALLINT fields.
        if (!empty($field['autoincrement'])) {
            return $this->getIntegerTypeDeclarationSQL($field);
        }

        return 'SMALLINT' . $this->_getCommonIntegerTypeDeclarationSQL($field);
    }

    /**
     * {@inheritDoc}
     */
    public function getMediumIntTypeDeclarationSql(array $field) {
        //  SQLite autoincrement is implicit for INTEGER PKs, but not for MEDIUMINT fields.
        if (!empty($field['autoincrement'])) {
            return $this->getIntegerTypeDeclarationSQL($field);
        }

        return 'MEDIUMINT' . $this->_getCommonIntegerTypeDeclarationSQL($field);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTypeDeclarationSQL(array $fieldDeclaration) {
        return 'DATETIME';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTypeDeclarationSQL(array $fieldDeclaration) {
        return 'DATE';
    }

    /**
     * {@inheritDoc}
     */
    public function getTimeTypeDeclarationSQL(array $fieldDeclaration) {
        return 'TIME';
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef) {
        // sqlite autoincrement is implicit for integer PKs, but not when the field is unsigned
        if (!empty($columnDef['autoincrement'])) {
            return '';
        }

        return !empty($columnDef['unsigned']) ? ' UNSIGNED' : '';
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyDeclarationSQL(ForeignKeyConstraint $foreignKey) {
        return parent::getForeignKeyDeclarationSQL(new ForeignKeyConstraint(
                        $foreignKey->getQuotedLocalColumns($this), str_replace('.', '__', $foreignKey->getQuotedForeignTableName($this)), $foreignKey->getQuotedForeignColumns($this), $foreignKey->getName(), $foreignKey->getOptions()
        ));
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCreateTableSQL($name, array $columns, array $options = array()) {
        $name = str_replace('.', '__', $name);
        $queryFields = $this->getColumnDeclarationListSQL($columns);

        if (isset($options['uniqueConstraints']) && !empty($options['uniqueConstraints'])) {
            foreach ($options['uniqueConstraints'] as $name => $definition) {
                $queryFields .= ', ' . $this->getUniqueConstraintDeclarationSQL($name, $definition);
            }
        }

        if (isset($options['primary']) && !empty($options['primary'])) {
            $keyColumns = array_unique(array_values($options['primary']));
            $queryFields.= ', PRIMARY KEY(' . implode(', ', $keyColumns) . ')';
        }

        if (isset($options['foreignKeys'])) {
            foreach ($options['foreignKeys'] as $foreignKey) {
                $queryFields.= ', ' . $this->getForeignKeyDeclarationSQL($foreignKey);
            }
        }

        $query[] = 'CREATE TABLE ' . $name . ' (' . $queryFields . ')';

        if (isset($options['alter']) && true === $options['alter']) {
            return $query;
        }

        if (isset($options['indexes']) && !empty($options['indexes'])) {
            foreach ($options['indexes'] as $indexDef) {
                $query[] = $this->getCreateIndexSQL($indexDef, $name);
            }
        }

        if (isset($options['unique']) && !empty($options['unique'])) {
            foreach ($options['unique'] as $indexDef) {
                $query[] = $this->getCreateIndexSQL($indexDef, $name);
            }
        }

        return $query;
    }

    /**
     * {@inheritDoc}
     */
    protected function getVarcharTypeDeclarationSQLSnippet($length, $fixed) {
        return $fixed ? ($length ? 'CHAR(' . $length . ')' : 'CHAR(255)') : ($length ? 'VARCHAR(' . $length . ')' : 'TEXT');
    }

    /**
     * {@inheritdoc}
     */
    protected function getBinaryTypeDeclarationSQLSnippet($length, $fixed) {
        return 'BLOB';
    }

    /**
     * {@inheritdoc}
     */
    public function getBinaryMaxLength() {
        return 0;
    }

    /**
     * {@inheritdoc}
     */
    public function getBinaryDefaultLength() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public function getClobTypeDeclarationSQL(array $field) {
        return 'CLOB';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableConstraintsSQL($table) {
        $table = str_replace('.', '__', $table);

        return "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name = '$table' AND sql NOT NULL ORDER BY name";
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableColumnsSQL($table, $currentDatabase = null) {
        
        return 'SELECT
                r.rdb$field_position AS field_position,
                trim(lower(r.RDB$FIELD_NAME)) AS field_name,
                trim(lower(CASE f.RDB$FIELD_TYPE
                WHEN 261 THEN \'BLOB\'
                WHEN 14 THEN \'CHAR\'
                WHEN 40 THEN \'CSTRING\'
                WHEN 11 THEN \'D_FLOAT\'
                WHEN 27 THEN \'DOUBLE\'
                WHEN 10 THEN \'FLOAT\'
                WHEN 16 THEN \'INT64\'
                WHEN 8 THEN \'INTEGER\'
                WHEN 9 THEN \'QUAD\'
                WHEN 7 THEN \'SMALLINT\'
                WHEN 12 THEN \'DATE\'
                WHEN 13 THEN \'TIME\'
                WHEN 35 THEN \'TIMESTAMP\'
                WHEN 37 THEN \'VARCHAR\'
                ELSE \'UNKNOWN\'
                END)) AS field_type,
                case when r.rdb$null_flag is null then 0 else 1 end NOT_NULL,
                REPLACE (r.rdb$default_source, \'DEFAULT \', \'\') default_value,
                case when s.rdb$field_position is null then 0 else s.rdb$field_position + 1 end pk,
                f.RDB$FIELD_LENGTH FIELD_LENGTH
            FROM RDB$RELATION_FIELDS r
            LEFT JOIN RDB$FIELDS f ON r.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
            LEFT JOIN RDB$COLLATIONS coll ON f.RDB$COLLATION_ID = coll.RDB$COLLATION_ID
            LEFT JOIN RDB$CHARACTER_SETS cset ON f.RDB$CHARACTER_SET_ID = cset.RDB$CHARACTER_SET_ID
            left join rdb$indices i
            on i.rdb$relation_name = r.rdb$relation_name
            and i.rdb$index_name like \'PK_%\'
            left join rdb$index_segments s
            on s.rdb$index_name = i.rdb$index_name
            and s.rdb$field_name = r.rdb$field_name
            WHERE lower(r.RDB$RELATION_NAME) = lower(\'' . $table . '\')
            ORDER BY r.RDB$FIELD_POSITION;';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableIndexesSQL($table, $currentDatabase = null) {

        return 'SELECT
                s.RDB$FIELD_POSITION AS field_position,
                trim(lower(rc.RDB$CONSTRAINT_NAME)) AS constraint_name,
                i.rdb$unique_flag as unique_field,
                case
                when rc.RDB$CONSTRAINT_NAME like \'UNQ%\' then \'U\'
                when rc.RDB$CONSTRAINT_NAME like \'IDX%\'then \'C\'
                when rc.RDB$CONSTRAINT_NAME like \'PK_%\'then \'PK\'
                end type_contraint,
                trim(lower(s.rdb$field_name)) field_name,
                \'0\' partial_index
                FROM RDB$INDEX_SEGMENTS s
                LEFT JOIN RDB$INDICES i ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
                LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME
                LEFT JOIN RDB$REF_CONSTRAINTS refc ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
                LEFT JOIN RDB$RELATION_CONSTRAINTS rc2 ON rc2.RDB$CONSTRAINT_NAME = refc.RDB$CONST_NAME_UQ
                LEFT JOIN RDB$INDICES i2 ON i2.RDB$INDEX_NAME = rc2.RDB$INDEX_NAME
                LEFT JOIN RDB$INDEX_SEGMENTS s2 ON i2.RDB$INDEX_NAME = s2.RDB$INDEX_NAME
                WHERE rc.RDB$CONSTRAINT_TYPE <> \'FOREIGN KEY\'
                and lower(i.RDB$RELATION_NAME) = lower(\'' . $table . '\')
                ORDER BY s.RDB$FIELD_POSITION';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTablesSQL() {
        return 'select trim(lower(rdb$relation_name)) as table_name from rdb$relations
                where rdb$system_flag = 0
                order by 1';
    }

    /**
     * {@inheritDoc}
     */
    public function getListViewsSQL($database) {
        return 'SELECT trim(lower(RDB$RELATION_NAME)) view_name FROM RDB$RELATIONS WHERE NOT RDB$VIEW_BLR IS NULL';
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateViewSQL($name, $sql) {
        return 'CREATE VIEW ' . $name . ' AS ' . $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getDropViewSQL($name) {
        return 'DROP VIEW ' . $name;
    }

    /**
     * {@inheritDoc}
     */
    public function getAdvancedForeignKeyOptionsSQL(ForeignKeyConstraint $foreignKey) {
        $query = parent::getAdvancedForeignKeyOptionsSQL($foreignKey);

        $query .= (($foreignKey->hasOption('deferrable') && $foreignKey->getOption('deferrable') !== false) ? ' ' : ' NOT ') . 'DEFERRABLE';
        $query .= ' INITIALLY ' . (($foreignKey->hasOption('deferred') && $foreignKey->getOption('deferred') !== false) ? 'DEFERRED' : 'IMMEDIATE');

        return $query;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsIdentityColumns() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsColumnCollation() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function getName() {
        return 'firebird';
    }

    /**
     * {@inheritDoc}
     */
    public function getTruncateTableSQL($tableName, $cascade = false) {
        $tableName = str_replace('.', '__', $tableName);

        return 'DELETE FROM ' . $tableName;
    }

    /**
     * User-defined function for Firebird that is used with PDO::firebirdCreateFunction().
     *
     * @param integer|float $value
     *
     * @return float
     */
    static public function udfSqrt($value) {
        return sqrt($value);
    }

    /**
     * User-defined function for Firebird that implements MOD(a, b).
     *
     * @param integer $a
     * @param integer $b
     *
     * @return integer
     */
    static public function udfMod($a, $b) {
        return ($a % $b);
    }

    /**
     * @param string  $str
     * @param string  $substr
     * @param integer $offset
     *
     * @return integer
     */
    static public function udfLocate($str, $substr, $offset = 0) {
        // SQL's LOCATE function works on 1-based positions, while PHP's strpos works on 0-based positions.
        // So we have to make them compatible if an offset is given.
        if ($offset > 0) {
            $offset -= 1;
        }

        $pos = strpos($str, $substr, $offset);

        if ($pos !== false) {
            return $pos + 1;
        }

        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public function getForUpdateSql() {
        return '';
    }

    /**
     * {@inheritDoc}
     */
    protected function initializeDoctrineTypeMappings() {
        $this->doctrineTypeMapping = array(
            'smallint' => 'smallint',
            'integer' => 'integer',
            'bigint' => 'bigint',
            'float' => 'float',
            'double precision' => 'double precision',
            'numeric' => 'decimal',
            'decimal' => 'decimal',
            'date' => 'date',
            'time' => 'time',
            'timestamp' => 'datetime',
            'datetime' => 'datetime',
            'boolean' => 'boolean',
            'tinyint' => 'boolean',
            'mediumint' => 'integer',
            'int' => 'integer',
            'serial' => 'integer',
            'bigserial' => 'bigint',
            'clob' => 'text',
            'tinytext' => 'text',
            'mediumtext' => 'text',
            'longtext' => 'text',
            'text' => 'text',
            'varchar' => 'string',
            'longvarchar' => 'string',
            'varchar2' => 'string',
            'nvarchar' => 'string',
            'image' => 'string',
            'ntext' => 'string',
            'char' => 'string',
            'double' => 'float',
            'real' => 'float',
            'blob' => 'blob',
        );
    }

    /**
     * {@inheritDoc}
     */
    protected function getReservedKeywordsClass() {
        return 'Doctrine\DBAL\Platforms\Keywords\FirebirdKeywords';
    }

    /**
     * {@inheritDoc}
     */
    protected function getPreAlterTableIndexForeignKeySQL(TableDiff $diff) {
        if (!$diff->fromTable instanceof Table) {
            throw new DBALException('Firebird platform requires for alter table the table diff with reference to original table schema');
        }

        $sql = array();
        foreach ($diff->fromTable->getIndexes() as $index) {
            if (!$index->isPrimary()) {
                $sql[] = $this->getDropIndexSQL($index, $diff->name);
            }
        }

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    protected function getPostAlterTableIndexForeignKeySQL(TableDiff $diff) {
        if (!$diff->fromTable instanceof Table) {
            throw new DBALException('Firebird platform requires for alter table the table diff with reference to original table schema');
        }

        $sql = array();
        $tableName = $diff->newName ? $diff->getNewName() : $diff->getName($this);
        foreach ($this->getIndexesInAlteredTable($diff) as $index) {
            if ($index->isPrimary()) {
                continue;
            }

            $sql[] = $this->getCreateIndexSQL($index, $tableName->getQuotedName($this));
        }

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset) {
        return $query;
    }

    /**
     * {@inheritDoc}
     */
    public function getBlobTypeDeclarationSQL(array $field) {
        return 'BLOB';
    }

    /**
     * {@inheritDoc}
     */
    public function getTemporaryTableName($tableName) {
        $tableName = str_replace('.', '__', $tableName);

        return $tableName;
    }

    /**
     * {@inheritDoc}
     *
     * Firebird Platform emulates schema by underscoring each dot and generating tables
     * into the default database.
     *
     * This hack is implemented to be able to use Firebird as testdriver when
     * using schema supporting databases.
     */
    public function canEmulateSchemas() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsForeignKeyConstraints() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function getCreatePrimaryKeySQL(Index $index, $table) {
        throw new DBALException('Firebird platform does not support alter primary key.');
    }

    /**
     * {@inheritdoc}
     */
    public function getCreateForeignKeySQL(ForeignKeyConstraint $foreignKey, $table) {
        throw new DBALException('Firebird platform does not support alter foreign key.');
    }

    /**
     * {@inheritdoc}
     */
    public function getDropForeignKeySQL($foreignKey, $table) {
        throw new DBALException('Firebird platform does not support alter foreign key.');
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateConstraintSQL(Constraint $constraint, $table) {
        throw new DBALException('Firebird platform does not support alter constraint.');
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateTableSQL(Table $table, $createFlags = null) {
        $createFlags = null === $createFlags ? self::CREATE_INDEXES | self::CREATE_FOREIGNKEYS : $createFlags;

        return parent::getCreateTableSQL($table, $createFlags);
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableForeignKeysSQL($table, $database = null) {
        $table = str_replace('.', '__', $table);

        return 'SELECT
                i.rdb$segment_count - 1 as count_,
                s2.rdb$field_position  KEY_SEQ,
                trim(lower(i2.RDB$RELATION_NAME)) AS references_table,
                trim(lower(s.RDB$FIELD_NAME)) AS field_name,
                trim(lower(s2.RDB$FIELD_NAME)) AS references_field,
                case when refc.RDB$UPDATE_RULE = \'RESTRICT\' then \'NO ACTION\' else refc.RDB$UPDATE_RULE end AS on_update,
                case when refc.RDB$DELETE_RULE = \'RESTRICT\' then \'NO ACTION\' else refc.RDB$UPDATE_RULE end AS on_delete,
                \'NONE\'
                FROM RDB$INDEX_SEGMENTS s
                LEFT JOIN RDB$INDICES i
                ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
                LEFT JOIN RDB$RELATION_CONSTRAINTS rc
                ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME
                LEFT JOIN RDB$REF_CONSTRAINTS refc
                ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
                LEFT JOIN RDB$RELATION_CONSTRAINTS rc2
                ON rc2.RDB$CONSTRAINT_NAME = refc.RDB$CONST_NAME_UQ
                LEFT JOIN RDB$INDICES i2
                ON i2.RDB$INDEX_NAME = rc2.RDB$INDEX_NAME
                LEFT JOIN RDB$INDEX_SEGMENTS s2
                ON i2.RDB$INDEX_NAME = s2.RDB$INDEX_NAME
                AND S2.RDB$FIELD_POSITION = S.RDB$FIELD_POSITION
                WHERE rc.RDB$CONSTRAINT_TYPE = \'FOREIGN KEY\'
                AND lower(i.RDB$RELATION_NAME) = lower(' . $table . ')
                ORDER BY 1,2';

        //return "PRAGMA foreign_key_list('$table')";
    }

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff) {
        $sql = $this->getSimpleAlterTableSQL($diff);
        if (false !== $sql) {
            return $sql;
        }

        $fromTable = $diff->fromTable;
        if (!$fromTable instanceof Table) {
            throw new DBALException('Firebird platform requires for alter table the table diff with reference to original table schema');
        }

        $table = clone $fromTable;

        $columns = array();
        $oldColumnNames = array();
        $newColumnNames = array();
        $columnSql = array();

        foreach ($table->getColumns() as $columnName => $column) {
            $columnName = strtolower($columnName);
            $columns[$columnName] = $column;
            $oldColumnNames[$columnName] = $newColumnNames[$columnName] = $column->getQuotedName($this);
        }

        foreach ($diff->removedColumns as $columnName => $column) {
            if ($this->onSchemaAlterTableRemoveColumn($column, $diff, $columnSql)) {
                continue;
            }

            $columnName = strtolower($columnName);
            if (isset($columns[$columnName])) {
                unset($columns[$columnName]);
                unset($oldColumnNames[$columnName]);
                unset($newColumnNames[$columnName]);
            }
        }

        foreach ($diff->renamedColumns as $oldColumnName => $column) {
            if ($this->onSchemaAlterTableRenameColumn($oldColumnName, $column, $diff, $columnSql)) {
                continue;
            }

            $oldColumnName = strtolower($oldColumnName);
            if (isset($columns[$oldColumnName])) {
                unset($columns[$oldColumnName]);
            }

            $columns[strtolower($column->getName())] = $column;

            if (isset($newColumnNames[$oldColumnName])) {
                $newColumnNames[$oldColumnName] = $column->getQuotedName($this);
            }
        }

        foreach ($diff->changedColumns as $oldColumnName => $columnDiff) {
            if ($this->onSchemaAlterTableChangeColumn($columnDiff, $diff, $columnSql)) {
                continue;
            }

            if (isset($columns[$oldColumnName])) {
                unset($columns[$oldColumnName]);
            }

            $columns[strtolower($columnDiff->column->getName())] = $columnDiff->column;

            if (isset($newColumnNames[$oldColumnName])) {
                $newColumnNames[$oldColumnName] = $columnDiff->column->getQuotedName($this);
            }
        }

        foreach ($diff->addedColumns as $columnName => $column) {
            if ($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }

            $columns[strtolower($columnName)] = $column;
        }

        $sql = array();
        $tableSql = array();
        if (!$this->onSchemaAlterTable($diff, $tableSql)) {
            $dataTable = new Table('__temp__' . $table->getName());

            $newTable = new Table($table->getQuotedName($this), $columns, $this->getPrimaryIndexInAlteredTable($diff), $this->getForeignKeysInAlteredTable($diff), 0, $table->getOptions());
            $newTable->addOption('alter', true);

//            $sql = $this->getPreAlterTableIndexForeignKeySQL($diff);
//            //$sql = array_merge($sql, $this->getCreateTableSQL($dataTable, 0));
//            $sql[] = sprintf('CREATE GLOBAL TEMPORARY TABLE %s AS SELECT %s FROM %s', $dataTable->getQuotedName($this), implode(', ', $oldColumnNames), $table->getQuotedName($this));
//            $sql[] = $this->getDropTableSQL($fromTable);
//
//            $sql = array_merge($sql, $this->getCreateTableSQL($newTable));
//            $sql[] = sprintf('INSERT INTO %s (%s) SELECT %s FROM %s', $newTable->getQuotedName($this), implode(', ', $newColumnNames), implode(', ', $oldColumnNames), $dataTable->getQuotedName($this));
//            $sql[] = $this->getDropTableSQL($dataTable);

            if ($diff->newName && $diff->newName != $diff->name) {
                $renamedTable = $diff->getNewName();
                $sql[] = 'ALTER TABLE ' . $newTable->getQuotedName($this) . ' RENAME TO ' . $renamedTable->getQuotedName($this);
            }

            $sql = array_merge($sql, $this->getPostAlterTableIndexForeignKeySQL($diff));
        }

        return array_merge($sql, $tableSql, $columnSql);
    }

    /**
     * @param \Doctrine\DBAL\Schema\TableDiff $diff
     *
     * @return array|bool
     */
    private function getSimpleAlterTableSQL(TableDiff $diff) {
        // Suppress changes on integer type autoincrement columns.
        foreach ($diff->changedColumns as $oldColumnName => $columnDiff) {
            if (!$columnDiff->fromColumn instanceof Column ||
                    !$columnDiff->column instanceof Column ||
                    !$columnDiff->column->getAutoincrement() ||
                    !(string) $columnDiff->column->getType() === 'Integer'
            ) {
                continue;
            }

            if (!$columnDiff->hasChanged('type') && $columnDiff->hasChanged('unsigned')) {
                unset($diff->changedColumns[$oldColumnName]);

                continue;
            }

            $fromColumnType = (string) $columnDiff->fromColumn->getType();

            if ($fromColumnType === 'SmallInt' || $fromColumnType === 'BigInt') {
                unset($diff->changedColumns[$oldColumnName]);
            }
        }

        if (!empty($diff->renamedColumns) || !empty($diff->addedForeignKeys) || !empty($diff->addedIndexes) || !empty($diff->changedColumns) || !empty($diff->changedForeignKeys) || !empty($diff->changedIndexes) || !empty($diff->removedColumns) || !empty($diff->removedForeignKeys) || !empty($diff->removedIndexes) || !empty($diff->renamedIndexes)
        ) {
            return false;
        }

        $table = new Table($diff->name);

        $sql = array();
        $tableSql = array();
        $columnSql = array();

        foreach ($diff->addedColumns as $column) {
            if ($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }

            $field = array_merge(array('unique' => null, 'autoincrement' => null, 'default' => null), $column->toArray());
            $type = (string) $field['type'];
            switch (true) {
                case isset($field['columnDefinition']) || $field['autoincrement'] || $field['unique']:
                case $type == 'DateTime' && $field['default'] == $this->getCurrentTimestampSQL():
                case $type == 'Date' && $field['default'] == $this->getCurrentDateSQL():
                case $type == 'Time' && $field['default'] == $this->getCurrentTimeSQL():
                    return false;
            }

            $field['name'] = $column->getQuotedName($this);
            if (strtolower($field['type']) == 'string' && $field['length'] === null) {
                $field['length'] = 255;
            }

            $sql[] = 'ALTER TABLE ' . $table->getQuotedName($this) . ' ADD COLUMN ' . $this->getColumnDeclarationSQL($field['name'], $field);
        }

        if (!$this->onSchemaAlterTable($diff, $tableSql)) {
            if ($diff->newName !== false) {
                $newTable = new Identifier($diff->newName);
                $sql[] = 'ALTER TABLE ' . $table->getQuotedName($this) . ' RENAME TO ' . $newTable->getQuotedName($this);
            }
        }

        return array_merge($sql, $tableSql, $columnSql);
    }

    /**
     * @param \Doctrine\DBAL\Schema\TableDiff $diff
     *
     * @return array
     */
    private function getColumnNamesInAlteredTable(TableDiff $diff) {
        $columns = array();

        foreach ($diff->fromTable->getColumns() as $columnName => $column) {
            $columns[strtolower($columnName)] = $column->getName();
        }

        foreach ($diff->removedColumns as $columnName => $column) {
            $columnName = strtolower($columnName);
            if (isset($columns[$columnName])) {
                unset($columns[$columnName]);
            }
        }

        foreach ($diff->renamedColumns as $oldColumnName => $column) {
            $columnName = $column->getName();
            $columns[strtolower($oldColumnName)] = $columnName;
            $columns[strtolower($columnName)] = $columnName;
        }

        foreach ($diff->changedColumns as $oldColumnName => $columnDiff) {
            $columnName = $columnDiff->column->getName();
            $columns[strtolower($oldColumnName)] = $columnName;
            $columns[strtolower($columnName)] = $columnName;
        }

        foreach ($diff->addedColumns as $columnName => $column) {
            $columns[strtolower($columnName)] = $columnName;
        }

        return $columns;
    }

    /**
     * @param \Doctrine\DBAL\Schema\TableDiff $diff
     *
     * @return \Doctrine\DBAL\Schema\Index[]
     */
    private function getIndexesInAlteredTable(TableDiff $diff) {
        $indexes = $diff->fromTable->getIndexes();
        $columnNames = $this->getColumnNamesInAlteredTable($diff);

        foreach ($indexes as $key => $index) {
            foreach ($diff->renamedIndexes as $oldIndexName => $renamedIndex) {
                if (strtolower($key) === strtolower($oldIndexName)) {
                    unset($indexes[$key]);
                }
            }

            $changed = false;
            $indexColumns = array();
            foreach ($index->getColumns() as $columnName) {
                $normalizedColumnName = strtolower($columnName);
                if (!isset($columnNames[$normalizedColumnName])) {
                    unset($indexes[$key]);
                    continue 2;
                } else {
                    $indexColumns[] = $columnNames[$normalizedColumnName];
                    if ($columnName !== $columnNames[$normalizedColumnName]) {
                        $changed = true;
                    }
                }
            }

            if ($changed) {
                $indexes[$key] = new Index($index->getName(), $indexColumns, $index->isUnique(), $index->isPrimary(), $index->getFlags());
            }
        }

        foreach ($diff->removedIndexes as $index) {
            $indexName = strtolower($index->getName());
            if (strlen($indexName) && isset($indexes[$indexName])) {
                unset($indexes[$indexName]);
            }
        }

        foreach (array_merge($diff->changedIndexes, $diff->addedIndexes, $diff->renamedIndexes) as $index) {
            $indexName = strtolower($index->getName());
            if (strlen($indexName)) {
                $indexes[$indexName] = $index;
            } else {
                $indexes[] = $index;
            }
        }

        return $indexes;
    }

    /**
     * @param \Doctrine\DBAL\Schema\TableDiff $diff
     *
     * @return array
     */
    private function getForeignKeysInAlteredTable(TableDiff $diff) {
        $foreignKeys = $diff->fromTable->getForeignKeys();
        $columnNames = $this->getColumnNamesInAlteredTable($diff);

        foreach ($foreignKeys as $key => $constraint) {
            $changed = false;
            $localColumns = array();
            foreach ($constraint->getLocalColumns() as $columnName) {
                $normalizedColumnName = strtolower($columnName);
                if (!isset($columnNames[$normalizedColumnName])) {
                    unset($foreignKeys[$key]);
                    continue 2;
                } else {
                    $localColumns[] = $columnNames[$normalizedColumnName];
                    if ($columnName !== $columnNames[$normalizedColumnName]) {
                        $changed = true;
                    }
                }
            }

            if ($changed) {
                $foreignKeys[$key] = new ForeignKeyConstraint($localColumns, $constraint->getForeignTableName(), $constraint->getForeignColumns(), $constraint->getName(), $constraint->getOptions());
            }
        }

        foreach ($diff->removedForeignKeys as $constraint) {
            $constraintName = strtolower($constraint->getName());
            if (strlen($constraintName) && isset($foreignKeys[$constraintName])) {
                unset($foreignKeys[$constraintName]);
            }
        }

        foreach (array_merge($diff->changedForeignKeys, $diff->addedForeignKeys) as $constraint) {
            $constraintName = strtolower($constraint->getName());
            if (strlen($constraintName)) {
                $foreignKeys[$constraintName] = $constraint;
            } else {
                $foreignKeys[] = $constraint;
            }
        }

        return $foreignKeys;
    }

    /**
     * @param \Doctrine\DBAL\Schema\TableDiff $diff
     *
     * @return array
     */
    private function getPrimaryIndexInAlteredTable(TableDiff $diff) {
        $primaryIndex = array();

        foreach ($this->getIndexesInAlteredTable($diff) as $index) {
            if ($index->isPrimary()) {
                $primaryIndex = array($index->getName() => $index);
            }
        }

        return $primaryIndex;
    }

}
