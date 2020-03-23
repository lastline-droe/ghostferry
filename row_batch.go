package ghostferry

import (
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
	PaginationKeyIndex() int
	ValuesContainPaginationKey() bool
	Fingerprints() map[uint64][]byte
}

type DataRowBatch struct {
	values             []RowData
	paginationKeyIndex int
	table              *TableSchema
	fingerprints       map[uint64][]byte
}

func NewDataRowBatch(table *TableSchema, values []RowData) *DataRowBatch {
	return NewDataRowBatchWithPaginationKey(table, values, -1)
}

func NewDataRowBatchWithPaginationKey(table *TableSchema, values []RowData, paginationKeyIndex int) *DataRowBatch {
	return &DataRowBatch{
		values:             values,
		paginationKeyIndex: paginationKeyIndex,
		table:              table,
	}
}

func (e *DataRowBatch) Values() []RowData {
	return e.values
}

func (e *DataRowBatch) PaginationKeyIndex() int {
	return e.paginationKeyIndex
}

func (e *DataRowBatch) ValuesContainPaginationKey() bool {
	return e.paginationKeyIndex >= 0
}

func (e *DataRowBatch) Size() int {
	return len(e.values)
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
