package ghostferry

import (
	"fmt"
	sql "github.com/Shopify/ghostferry/sqlwrapper"

	"github.com/sirupsen/logrus"
)

type BatchWriterVerificationFailed struct {
	mismatchedPaginationKeys []uint64
	table                    string
}

func (e BatchWriterVerificationFailed) Error() string {
	return fmt.Sprintf("row fingerprints for paginationKeys %v on %v do not match", e.mismatchedPaginationKeys, e.table)
}

type BatchWriter struct {
	DB             *sql.DB
	InlineVerifier *InlineVerifier
	StateTracker   *StateTracker

	DatabaseRewrites map[string]string
	TableRewrites    map[string]string

	WriteRetries int

	stmtCache *StmtCache
	logger    *logrus.Entry
}

func (w *BatchWriter) Initialize() {
	w.stmtCache = NewStmtCache()
	w.logger = logrus.WithField("tag", "batch_writer")
}

func (w *BatchWriter) WriteRowBatch(batch RowBatch) error {
	return WithRetries(w.WriteRetries, 0, w.logger, "write batch to target", func() (err error) {
		db := batch.TableSchema().Schema
		if targetDbName, exists := w.DatabaseRewrites[db]; exists {
			db = targetDbName
		}

		table := batch.TableSchema().Name
		if targetTableName, exists := w.TableRewrites[table]; exists {
			table = targetTableName
		}

		if batch.Size() == 0 {
			w.logger.Debug("ignoring empty row-batch for %s.%s", db, table)
			return
		}

		txInUse := false
		tx, dbErr := w.DB.Begin()
		if dbErr != nil {
			err = fmt.Errorf("unable to begin transaction in BatchWriter: %v", dbErr)
			return
		}

		// make sure the transaction gets abandoned if we didn't commit it
		defer func() {
			if tx != nil {
				w.logger.Debugf("rolling back transaction: %s", err)
				tx.Rollback()
			}
		}()

		query, args, dbErr := batch.AsSQLQuery(db, table)
		if dbErr != nil {
			err = fmt.Errorf("during generating sql batch query: %v", dbErr)
			return
		}

		txUpdated, dbErr := w.queueStatement(tx, query, args)
		if dbErr != nil {
			err = dbErr
			return
		}
		if txUpdated {
			txInUse = true
		}

		// Note that the state tracker expects us the track based on the original
		// database and table names as opposed to the target ones.
		stateTableName := batch.TableSchema().String()

		switch b := batch.(type) {
		case InsertRowBatch:
			endPaginationKeypos, txUpdated, insertErr := w.handleInsertRowBatch(tx, b, db, table)
			if insertErr != nil {
				err = insertErr
				return
			}
			if txUpdated {
				txInUse = true
			}

			if w.StateTracker != nil {
				defer func() {
					if err == nil {
						w.StateTracker.UpdateLastSuccessfulPaginationKey(stateTableName, endPaginationKeypos)
					}
				}()
			}
		}

		if batch.IsTableComplete() && w.StateTracker != nil {
			query, args, stateErr := w.StateTracker.GetStoreRowCopyDoneSql(stateTableName)
			if stateErr != nil {
				err = fmt.Errorf("during generating row-copy done: %v", stateErr)
				return
			}

			txUpdated, dbErr := w.queueStatement(tx, query, args)
			if dbErr != nil {
				err = dbErr
				return
			}
			if txUpdated {
				txInUse = true
			}

			defer func() {
				if err == nil {
					w.StateTracker.MarkTableAsCompleted(stateTableName)
				}
			}()
		}

		if txInUse {
			err = tx.Commit()
			if err != nil {
				err = fmt.Errorf("during row-copy commit (%s): %v", query, err)
			} else {
				// avoid rolling it back (too late anyways) on function exit
				tx = nil
			}
		} else {
			// we never really added any statement to the transaction - no need
			// to commit it. This should practically never happen, but let's be
			// on the safe side
			w.logger.Debug("discarding empty transaction")
		}

		return
	})
}

func (w *BatchWriter) handleInsertRowBatch(tx *sql.Tx, batch InsertRowBatch, db, table string) (endPaginationKeypos uint64, txUpdated bool, err error) {
	if !batch.ValuesContainPaginationKey() {
		return
	}

	values := batch.Values()
	index := batch.PaginationKeyIndex()
	startPaginationKeypos, err := values[0].GetUint64(index)
	if err != nil {
		return
	}

	endPaginationKeypos, err = values[len(values)-1].GetUint64(index)
	if err != nil {
		return
	}

	if w.InlineVerifier != nil {
		mismatches, verfierErr := w.InlineVerifier.CheckFingerprintInline(tx, db, table, batch)
		if err != nil {
			err = fmt.Errorf("during fingerprint checking for paginationKey %v -> %v: %v", startPaginationKeypos, endPaginationKeypos, verfierErr)
			return
		}
		if mismatches != nil && len(mismatches) > 0 {
			err = BatchWriterVerificationFailed{mismatches, batch.TableSchema().String()}
			return
		}
	}

	if w.StateTracker != nil {
		// Note that the state tracker expects us the track based on the original
		// database and table names as opposed to the target ones.
		query, args, stateErr := w.StateTracker.GetStoreRowCopyPositionSql(batch.TableSchema().String(), endPaginationKeypos)
		if stateErr != nil {
			err = fmt.Errorf("during generating row-copy position for paginationKey %v -> %v: %v", startPaginationKeypos, endPaginationKeypos, stateErr)
			return
		}

		txUpdatedForState, stateErr := w.queueStatement(tx, query, args)
		if stateErr != nil {
			err = fmt.Errorf("during generating row-copy position for paginationKey %v -> %v: %v", startPaginationKeypos, endPaginationKeypos, stateErr)
			return
		}
		if txUpdatedForState {
			txUpdated = true
		}
	}

	return
}


func (w *BatchWriter) queueStatement(tx *sql.Tx, query string, args []interface{}) (txUpdated bool, err error) {
	if query == "" {
		return
	}

	stmt, stmtErr := w.stmtCache.StmtFor(w.DB, query)
	if stmtErr != nil {
		err = stmtErr
		return
	}

	_, err = tx.Stmt(stmt).Exec(args...)
	if err != nil {
		err = fmt.Errorf("during copy statement: %v", err)
		return
	}

	txUpdated = true
	return
}
