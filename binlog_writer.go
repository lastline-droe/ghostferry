package ghostferry

import (
	"fmt"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"sync"

	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

var shutdownEvent = fmt.Errorf("binlog-writer shutting down")

type BinlogWriter struct {
	DB               *sql.DB
	DatabaseRewrites map[string]string
	TableRewrites    map[string]string
	Throttler        Throttler

	BatchSize          int
	WriteRetries       int
	ApplySchemaChanges bool
	LockStrategy       string

	ErrorHandler                ErrorHandler
	StateTracker                *StateTracker
	ForceResumeStateUpdatesToDB bool

	CopyFilter  CopyFilter
	TableFilter TableFilter
	TableSchema TableSchemaCache

	queryAnalyzer     *QueryAnalyzer
	binlogEventBuffer chan *ReplicationEvent
	logger            *logrus.Entry
}

func (b *BinlogWriter) Run() {
	b.logger = logrus.WithField("tag", "binlog_writer")
	b.queryAnalyzer = NewQueryAnalyzer()
	b.binlogEventBuffer = make(chan *ReplicationEvent, b.BatchSize)

	batch := make([]DXLEventWrapper, 0, b.BatchSize)
	for {
		if IncrediblyVerboseLogging {
			b.logger.Debugf("Have %d/%d elements in batch, waiting for elements from binlog queue", len(batch), b.BatchSize)
		}

		var replicationEvent *ReplicationEvent
		if len(batch) == 0 {
			// if we don't have anything in the batch yet, do a blocking read
			replicationEvent = <-b.binlogEventBuffer
			if replicationEvent == nil {
				// Channel is closed, no more events to write
				b.logger.Debugf("Binlog queue closed")
				break
			}
		} else {
			// otherwise see if we can add to the batch, but without blocking
			select {
			case ev := <-b.binlogEventBuffer:
				// got more elements - keep adding to the batch
				replicationEvent = ev
			default:
				// no more elements in the queue, apply the batch now
			}
			if replicationEvent == nil {
				// receiving events would have blocked - commit the batch and
				// block for new data in the queue
				b.logger.Debugf("Commit of batch %d/%d elements on empty queue", len(batch), b.BatchSize)
				b.applyBatch(batch)
				batch = make([]DXLEventWrapper, 0, b.BatchSize)
				continue
			}
		}

		if IncrediblyVerboseLogging {
			b.logger.Debugf("Received element from binlog queue: %v", replicationEvent)
		}

		dxlEvents, err := b.handleReplicationEvent(replicationEvent)
		if err == shutdownEvent {
			b.logger.Debugf("Commit of batch %d/%d elements on shutdown event", len(batch), b.BatchSize)
			b.applyBatch(batch)
			break
		} else if err != nil {
			b.ErrorHandler.Fatal("binlog_writer", err)
		}

		for _, dxlEvent := range dxlEvents {
			// if the event contains a statement that will create its own
			// transaction (typically DDL statements), commit what we have now
			//
			// all other batches are allowed to be "merged", because applying
			// the same DML events twice (e.g., if resuming from an earlier
			// position due to a missed saving of a binlog position) is safe due
			// to how we generate DML update statement
			if len(batch) > 0 && dxlEvent.DXLEvent.IsAutoTransaction() {
				b.logger.Debugf("Forcing commit of batch %d/%d elements", len(batch), b.BatchSize)
				b.applyBatch(batch)
				batch = make([]DXLEventWrapper, 0, b.BatchSize)
			}

			if IncrediblyVerboseLogging {
				b.logger.Debugf("Queuing DXL event %v to batch of %d/%d elements", dxlEvent, len(batch), b.BatchSize)
			}
			batch = append(batch, dxlEvent)
			if len(batch) >= b.BatchSize {
				b.logger.Debugf("Commit of batch %d/%d elements on full batch", len(batch), b.BatchSize)
				b.applyBatch(batch)
				batch = make([]DXLEventWrapper, 0, b.BatchSize)
			}
		}
	}
}

func (b *BinlogWriter) applyBatch(batch []DXLEventWrapper) {
	if len(batch) == 0 {
		return
	}

	err := WithRetries(b.WriteRetries, 0, b.logger, "write events to target", func() error {
		return b.writeEvents(batch)
	})
	if err != nil {
		b.ErrorHandler.Fatal("binlog_writer", err)
	}

	for _, dxlEvent := range batch {
		if dxlEvent.PostApplyCallback != nil {
			err := dxlEvent.PostApplyCallback.Notify()
			if err != nil {
				b.logger.Errorf("Invoking post-apply callback on %v failed: %s", dxlEvent, err)
				b.ErrorHandler.Fatal("binlog_writer", err)
			}
		}
	}
}

func (b *BinlogWriter) Stop() {
	close(b.binlogEventBuffer)
}

func (b *BinlogWriter) BufferBinlogEvents(event *ReplicationEvent) error {
	b.binlogEventBuffer <- event
	return nil
}

type DXLEventCallback interface {
	Notify() error
}

type ReloadTableSchemasCallback struct {
	*BinlogWriter
	TableStructuresToReload []*QualifiedTableName
}

func (c *ReloadTableSchemasCallback) Notify() error {
	for _, table := range c.TableStructuresToReload {
		err := c.BinlogWriter.ReloadTableSchema(table)
		if err != nil {
			return err
		}

		// since we block the binlog writing when an alteration of
		// schemas occurs, we can assume that all tables have been
		// copied completely.
		// This is particularly important if a new table is created,
		// since we want to make sure we don't start a copy when we
		// stop and resume the migration (via data_iterator and
		// batch_writer)
		err = c.BinlogWriter.MarkTableAsCopied(table)
		if err != nil {
			return err
		}
	}
	return nil
}

type DXLEventWrapper struct {
	DXLEvent
	*ReplicationEvent
	PostApplyCallback DXLEventCallback
}

func (b *BinlogWriter) handleRowsEvent(ev *ReplicationEvent, rowsEvent *replication.RowsEvent) ([]DXLEventWrapper, error) {
	events := make([]DXLEventWrapper, 0)

	table := b.TableSchema.Get(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))
	if table == nil {
		return events, nil
	}

	dmlEvs, err := NewBinlogDMLEvents(table, ev.BinlogEvent, ev.BinlogPosition, ev.EventTime)
	if err != nil {
		return events, err
	}

	for _, dmlEv := range dmlEvs {
		if b.CopyFilter != nil {
			applicable, err := b.CopyFilter.ApplicableDMLEvent(dmlEv)
			if err != nil {
				b.logger.WithError(err).Error("failed to apply filter for event")
				return events, err
			}
			if !applicable {
				continue
			}
		}

		events = append(events, DXLEventWrapper{DXLEvent: dmlEv, ReplicationEvent: ev})
		b.logger.WithFields(logrus.Fields{
			"database": dmlEv.Database(),
			"table":    dmlEv.Table(),
		}).Debugf("received event %T at %v", dmlEv, ev.EventTime)

		metrics.Count("RowEvent", 1, []MetricTag{
			MetricTag{"table", dmlEv.Table()},
			MetricTag{"source", "binlog"},
		}, 1.0)
	}

	return events, nil
}

func (b *BinlogWriter) handleQueryEvent(ev *ReplicationEvent, queryEvent *replication.QueryEvent) ([]DXLEventWrapper, error) {
	schemaEvents, err := b.queryAnalyzer.ParseSchemaChanges(string(queryEvent.Query), string(queryEvent.Schema))
	if err != nil {
		return nil, err
	}

	events := make([]DXLEventWrapper, 0)
	tableStructuresToReload := make([]*QualifiedTableName, 0)
	for _, schemaEvent := range schemaEvents {
		if !b.ApplySchemaChanges {
			b.logger.Warnf("ignoring schema event for %s: disabled", schemaEvent.AffectedTable)
			return events, nil
		}

		applicableDatabases, err := b.TableFilter.ApplicableDatabases([]string{schemaEvent.AffectedTable.SchemaName})
		if err != nil {
			b.logger.WithError(err).Errorf("could not apply database filter on %s", schemaEvent.AffectedTable)
			return events, err
		}
		if len(applicableDatabases) == 0 {
			b.logger.Infof("Ignoring schema change of %s: not an applicable DB", schemaEvent.AffectedTable)
			continue
		}

		// Does this SQL statement change the schema of the DB?
		if schemaEvent.IsSchemaChange {
			// we need to handle all schema changes, except those that *only*
			// drop a table, as we don't need to re-parse (actually, we can't)
			//the new schema after the schema change has been applied
			var tableToReload *QualifiedTableName
			if schemaEvent.DeletedTable == nil {
				// a table was created or altered. In either case, the "affected
				// table" is what we need to reload
				//
				// XXX: Should we mark the old table as invalid somehow if this
				// is actually a rename?
				tableToReload = schemaEvent.AffectedTable
			} else {
				// a table was either deleted or renamed. We do not care about
				// deletions, as we can't load the new schema anyways
				//
				// XXX: Should we mark the deleted table as invalid somehow?
				tableToReload = schemaEvent.CreatedTable
			}
			if tableToReload != nil {
				tableStructuresToReload = append(tableStructuresToReload, tableToReload)
			}
		}

		ddlEv, err := NewBinlogDDLEvent(schemaEvent.SchemaStatement, schemaEvent.AffectedTable, ev.BinlogPosition, ev.EventTime)
		if err != nil {
			return events, err
		}
		b.logger.WithFields(logrus.Fields{
			"database": ddlEv.Database(),
			"table":    ddlEv.Table(),
		}).Debugf("received event %T at %v", ddlEv, ev.EventTime)

		wrapper := DXLEventWrapper{
			DXLEvent:             ddlEv,
			ReplicationEvent:     ev,
			// NOTE: We need to delay the extraction of the altered schema
			// until after it has been applied. We don't know how far ahead the
			// source (master DB) we read from might be, and the target DB has
			// no (or an outdated) schema
			PostApplyCallback: &ReloadTableSchemasCallback{
				BinlogWriter:            b,
				TableStructuresToReload: tableStructuresToReload,
			},
		}
		events = append(events, wrapper)

		metrics.Count("SchemaEvent", 1, []MetricTag{
			MetricTag{"table", ddlEv.Table()},
			MetricTag{"source", "binlog"},
		}, 1.0)
	}

	return events, nil
}

func (b *BinlogWriter) handleReplicationEvent(ev *ReplicationEvent) ([]DXLEventWrapper, error) {
	if IncrediblyVerboseLogging {
		b.logger.Debugf("Handling replication event: %v", ev)
	}
	switch event := ev.BinlogEvent.Event.(type) {
	case *replication.RowsEvent:
		return b.handleRowsEvent(ev, event)
	case *replication.QueryEvent:
		return b.handleQueryEvent(ev, event)
	default:
		return nil, fmt.Errorf("unsupported replication event at pos %v: %T", ev.BinlogPosition, ev.BinlogEvent)
	}
}

func (b *BinlogWriter) ReloadTableSchema(table *QualifiedTableName) error {
	b.logger.Infof("Re-loading schema of %s from target DB", table)
	tableSchema, err := schema.NewTableFromSqlDB(b.DB.DB, table.SchemaName, table.TableName)
	if err != nil {
		return err
	}

	existingTable := b.TableSchema.Get(table.SchemaName, table.TableName)
	if existingTable == nil {
		b.logger.Infof("Initializing schema of %s.%s from target DB", table.SchemaName, table.TableName)
		b.TableSchema[table.String()] = &TableSchema{
			Table: tableSchema,
		}
	} else {
		existingTable.Table = tableSchema
	}

	return nil
}

func (b *BinlogWriter) MarkTableAsCopied(table *QualifiedTableName) error {
	b.logger.Infof("Notifying copy process of %s schema in target DB", table)
	query, args, err := b.StateTracker.GetStoreRowCopyDoneSql(table.String())
	if err != nil {
		b.logger.WithField("err", err).Errorf("Generating copy-done SQL for %s failed", table)
		return err
	}

	if query == "" {
		b.logger.Debug("Skip applying copy-done statement: state writer opt-out")
	} else {
		if IncrediblyVerboseLogging {
			b.logger.Debugf("Applying copy-done statement: %s (%v)", query, args)
		}
		_, err = b.DB.Exec(query, args...)
		if err != nil {
			b.logger.WithField("err", err).Errorf("Applying copy-done SQL for %s failed", table)
			return err
		}
	}

	b.StateTracker.MarkTableAsCompleted(table.String())
	return nil
}

func (b *BinlogWriter) writeEvents(events []DXLEventWrapper) error {
	WaitForThrottle(b.Throttler)

	queryBuffer := []byte("BEGIN;\n")
	locksToObtain := make(map[string]*sync.RWMutex)

	for _, ev := range events {
		eventDatabaseName := ev.DXLEvent.Database()
		if targetDatabaseName, exists := b.DatabaseRewrites[eventDatabaseName]; exists {
			eventDatabaseName = targetDatabaseName
		}

		eventTableName := ev.DXLEvent.Table()
		if targetTableName, exists := b.TableRewrites[eventTableName]; exists {
			eventTableName = targetTableName
		}

		sql, err := ev.DXLEvent.AsSQLString(eventDatabaseName, eventTableName)
		if err != nil {
			return fmt.Errorf("generating sql query at pos %v: %v", ev.DXLEvent.BinlogPosition(), err)
		}

		queryBuffer = append(queryBuffer, sql...)
		queryBuffer = append(queryBuffer, ";\n"...)

		// for DML events, we need to make sure we synchronize with the
		// data-iterator - for details on why, see the corresponding
		// data-iterator code
		if b.LockStrategy == LockTypeInGhostferry {
			if dmlEvent, ok := ev.DXLEvent.(DMLEvent); ok {
				fullTableName := dmlEvent.TableSchema().Table.String()
				if _, found := locksToObtain[fullTableName]; !found {
					locksToObtain[fullTableName] = b.StateTracker.GetTableLock(fullTableName)
				}
			}
		}
	}

	startEv := events[0].ReplicationEvent
	endEv := events[len(events)-1].ReplicationEvent

	var args []interface{}
	if b.ForceResumeStateUpdatesToDB && b.StateTracker != nil {
		var sql string
		var err error
		sql, args, err = b.StateTracker.GetStoreBinlogWriterPositionSql(endEv.BinlogPosition, endEv.EventTime)
		if err != nil {
			return nil
		}
		if sql != "" {
			queryBuffer = append(queryBuffer, sql...)
			queryBuffer = append(queryBuffer, ";\n"...)
		}
	}

	queryBuffer = append(queryBuffer, "COMMIT"...)
	query := string(queryBuffer)
	if IncrediblyVerboseLogging {
		b.logger.Debugf("Applying binlog statements: %s (%v)", query, args)
	}

	for _, lock := range locksToObtain {
		if lock != nil {
			lock.Lock()
			defer lock.Unlock()
		}
	}

	_, err := b.DB.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("exec query at pos %v -> %v (%d bytes): %v", startEv.BinlogPosition, endEv.BinlogPosition, len(query), err)
	}

	if b.StateTracker != nil {
		b.StateTracker.UpdateLastWrittenBinlogPosition(endEv.BinlogPosition)
	}

	return nil
}
