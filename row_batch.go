package ghostferry

import (
	"fmt"
	"strings"
)

type RowBatch interface {
	TableSchema() *TableSchema
	AsSQLQuery(schemaName, tableName string) (string, []interface{}, error)
	Size() int
	IsTableComplete() bool
}

type InsertRowBatch interface {
	RowBatch
	Values() []RowData
	VerifierPaginationKey(int) (uint64, error)
	Fingerprints() map[uint64][]byte
}

type DataRowBatch struct {
	values        []RowData
	table         *TableSchema
	fingerprints  map[uint64][]byte
}

func NewDataRowBatch(table *TableSchema, values []RowData) *DataRowBatch {
	return &DataRowBatch{
		values: values,
		table:  table,
	}
}

func (e *DataRowBatch) Values() []RowData {
	return e.values
}

func (e *DataRowBatch) Size() int {
	return len(e.values)
}

func (e *DataRowBatch) VerifierPaginationKey(rowIndex int) (paginationValue uint64, err error) {
	if e.table.PaginationKey == nil {
		err = fmt.Errorf("table %s does not have a pagination key", e.table)
	} else if e.table.PaginationKey.IsLinearUnsignedKey() {
		var value int64
		value, err = e.values[rowIndex].GetInt64(e.table.PaginationKey.ColumnIndices[0])
		if err == nil {
			// for legacy-compatibility, we allow signed pagination keys, so we have to
			// make sure no signed data snuck in
			if value < 0 {
				err = fmt.Errorf("table %s contains an unsupported (signed) pagination key value %d", e.table, value)
			} else {
				paginationValue = uint64(value)
			}
		}
	} else {
		err = UnsupportedPaginationKeyError(e.table.Schema, e.table.Name, e.table.PaginationKey.String())
	}

	return
}

func (e *DataRowBatch) IsTableComplete() bool {
	// we currently always use a dedicated batch for marking completion
	return false
}

func (e *DataRowBatch) TableSchema() *TableSchema {
	return e.table
}

func (e *DataRowBatch) Fingerprints() map[uint64][]byte {
	return e.fingerprints
}

func (e *DataRowBatch) AsSQLQuery(schemaName, tableName string) (string, []interface{}, error) {
	if err := verifyValuesHasTheSameLengthAsColumns(e.table, e.values...); err != nil {
		return "", nil, err
	}

	columns := quotedColumnNames(e.table)

	valuesStr := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	valuesStr = strings.Repeat(valuesStr+",", len(e.values)-1) + valuesStr

	query := "INSERT IGNORE INTO " +
		QuotedTableNameFromString(schemaName, tableName) +
		" (" + strings.Join(columns, ",") + ") VALUES " + valuesStr

	return query, e.flattenRowData(), nil
}

func (e *DataRowBatch) flattenRowData() []interface{} {
	rowSize := len(e.values[0])
	flattened := make([]interface{}, rowSize*len(e.values))

	for rowIdx, row := range e.values {
		for colIdx, col := range row {
			flattened[rowIdx*rowSize+colIdx] = col
		}
	}

	return flattened
}

type TruncateTableBatch struct {
	table *TableSchema
}

func NewTruncateTableBatch(table *TableSchema) *TruncateTableBatch {
	return &TruncateTableBatch{table}
}

func (e *TruncateTableBatch) TableSchema() *TableSchema {
	return e.table
}

func (e *TruncateTableBatch) Size() int {
	return 1
}

func (e *TruncateTableBatch) IsTableComplete() bool {
	// we currently always use a dedicated batch for marking completion
	return false
}

func (e *TruncateTableBatch) AsSQLQuery(schemaName, tableName string) (string, []interface{}, error) {
	quotedTableName := QuotedTableNameFromString(schemaName, tableName)
	quotedTableName = strings.Replace(quotedTableName, "*/", "", -1)
	query := "TRUNCATE /* ghostferry initialize table " + quotedTableName + " */ TABLE " + quotedTableName
	return query, nil, nil
}

type FinalizeTableCopyBatch struct {
	table *TableSchema
}

func NewFinalizeTableCopyBatch(table *TableSchema) *FinalizeTableCopyBatch {
	return &FinalizeTableCopyBatch{table}
}

func (e *FinalizeTableCopyBatch) TableSchema() *TableSchema {
	return e.table
}

func (e *FinalizeTableCopyBatch) Size() int {
	return 1
}

func (e *FinalizeTableCopyBatch) IsTableComplete() bool {
	return true
}

func (e *FinalizeTableCopyBatch) AsSQLQuery(schemaName, tableName string) (string, []interface{}, error) {
	quotedTableName := QuotedTableNameFromString(schemaName, tableName)
	quotedTableName = strings.Replace(quotedTableName, "*/", "", -1)
	query := "SELECT /* ghostferry finalize table " + quotedTableName + " */ 1"
	return query, nil, nil
}
