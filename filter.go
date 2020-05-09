package ghostferry

import (
	sq "github.com/Masterminds/squirrel"
)

// CopyFilter provides an interface for restricting the copying to a subset of
// data. This typically involves adding a WHERE condition in the ConstrainSelect
// function, and returning false for unwanted rows in ApplicableDMLEvent.
type CopyFilter interface {
	// BuildSelect is used to set up the query used for batch data copying,
	// allowing for restricting copying to a subset of data. Returning an error
	// here will cause the query to be retried, until the retry limit is
	// reached, at which point the ferry will be aborted. BuildSelect is passed
	// the columns to be selected, table being copied, the last primary key value
	// from the previous batch, the batch size, and whether we iterate in
	// descending order. Call DefaultBuildSelect to generate the default query,
	// which may be used as a starting point.
	BuildSelect([]string, *TableSchema, *PaginationKeyData, uint64, bool) (sq.SelectBuilder, error)

	// ApplicableEvent is used to filter events for rows that have been
	// filtered in ConstrainSelect. ApplicableEvent should return true if the
	// event is for a row that would be selected by ConstrainSelect, and false
	// otherwise.
	// Returning an error here will cause the ferry to be aborted.
	ApplicableDMLEvent(DMLEvent) (bool, error)
}

type TableFilter interface {
	ApplicableTables([]*TableSchema) ([]*TableSchema, error)
	ApplicableDatabases([]string) ([]string, error)
}
