package ghostferry

import (
	"fmt"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"sync"

	"github.com/sirupsen/logrus"
)

type DataIterator struct {
	DB                *sql.DB
	Concurrency       int
	SelectFingerprint bool

	ErrorHandler ErrorHandler
	CursorConfig *CursorConfig
	StateTracker *StateTracker

	targetPaginationKeys *sync.Map
	failOnFirstCopyError bool
	lockStrategy         string
	batchListeners       []func(RowBatch) error
	doneListeners        []func() error
	logger               *logrus.Entry
}

func NewDataIterator(f *Ferry) *DataIterator {
	d := &DataIterator{
		DB:                f.SourceDB,
		Concurrency:       f.Config.DataIterationConcurrency,
		SelectFingerprint: f.Config.VerifierType == VerifierTypeInline,

		ErrorHandler: f.ErrorHandler,
		CursorConfig: &CursorConfig{
			DB:        f.SourceDB,
			Throttler: f.Throttler,

			BatchSize:   f.Config.DataIterationBatchSize,
			ReadRetries: f.Config.DBReadRetries,

			IterateInDescendingOrder: f.Config.IterateInDescendingOrder,
		},
		StateTracker: f.StateTracker,

		failOnFirstCopyError: f.Config.FailOnFirstTableCopyError,
		lockStrategy:         f.Config.LockStrategy,
	}
	d.ensureInitialized()
	return d
}

func (d *DataIterator) ensureInitialized() {
	// helper in case a caller does not use NewDataIterator(), which we do in
	// tests. So we need to use this method for methods called for public
	// methods
	if d.targetPaginationKeys == nil {
		d.targetPaginationKeys = &sync.Map{}
	}
	if d.logger == nil {
		d.logger = logrus.WithField("tag", "data_iterator")
	}
}

func (d *DataIterator) Run(tables []*TableSchema) {
	d.ensureInitialized()
	// If a state tracker is not provided, then the caller doesn't care about
	// tracking state. However, some methods are still useful so we initialize
	// a minimal local instance.
	if d.StateTracker == nil {
		d.StateTracker = NewStateTracker(0)
	}

	d.logger.WithField("tablesCount", len(tables)).Info("starting data iterator run")
	paginatedTables, unpaginatedTables, err := GetTargetPaginationKeys(d.DB, tables, d.CursorConfig.IterateInDescendingOrder, d.logger)
	if err != nil {
		d.ErrorHandler.Fatal("data_iterator", err)
	}

	tmp := unpaginatedTables[:0]
	for _, table := range unpaginatedTables {
		tableName := table.String()
		if d.StateTracker.IsTableComplete(tableName) {
			// In a previous run, the table may have been completed.
			// We don't need to reiterate those tables as it has already been done.
			d.logger.WithField("table", tableName).Debug("table already copied completely, removing from unpaginagted table copy list")
		} else {
			tmp = append(tmp, table)
		}
	}
	unpaginatedTables = tmp

	for table, targetPaginationKey := range paginatedTables {
		tableName := table.String()
		if d.StateTracker.IsTableComplete(tableName) {
			// In a previous run, the table may have been completed.
			// We don't need to reiterate those tables as it has already been done.
			d.logger.WithField("table", tableName).Debug("table already copied completely, removing from paginagted table copy list")
			delete(paginatedTables, table)
		} else {
			d.targetPaginationKeys.Store(table.String(), targetPaginationKey)
		}
	}

	// we allow delaying raising an error until we have at least attempted to
	// copy every table. We may have many large tables and don't want to give
	// up immediately. If any error arises, we log it and re-raise it once all
	// tables have been copied or errored out.
	// This is particularly useful if there are temporary issues with the copy,
	// such as schemas changing on the source, but the copy races with the
	// schema being altered on the target as well.
	//
	// XXX: We should detect changes in schemas of copy-in-progress tables and
	// retry resuming the copy. For this we would simply have to re-queue a
	// table to copy and, once it is re-read from the channel, wait until the
	// schema has been updates. We could track a "schema version" for each
	// table to track any incoming schema changes and wait for this version to
	// change before re-attempting the copy. Of course there could be mulitple
	// schema versions that we have to skip, but that should be simple to
	// handle by trying and waiting multiple times.
	var lastError error

	paginatedTablesQueue := make(chan *TableSchema)
	unpaginatedTablesQueue := make(chan *TableSchema)
	wg := &sync.WaitGroup{}
	wg.Add(d.Concurrency + 1)

	for i := 0; i < d.Concurrency; i++ {
		go func(i int) {
			defer wg.Done()

			logger := d.logger.WithField("copy-instance", fmt.Sprintf("paginated-%d", i))
			for {
				table, ok := <-paginatedTablesQueue
				if !ok {
					break
				}

				tableLogger := logger.WithField("table", table.String())
				tableLogger.Info("starting to process table")

				err = d.processPaginatedTable(table)
				if err != nil {
					switch e := err.(type) {
					case BatchWriterVerificationFailed:
						tableLogger.WithField("incorrect_tables", e.table).Error(e.Error())
						d.ErrorHandler.Fatal("inline_verifier", err)
					default:
						tableLogger.WithError(err).Error("failed to iterate table")
						if d.failOnFirstCopyError {
							d.ErrorHandler.Fatal("data_iterator", err)
						}
						tableLogger.Warn("suspending error until all table copies have been attempted")
						lastError = err
					}
				} else {
					tableLogger.Info("done processing table")
				}
			}

			logger.Info("copier shutting down")
		}(i)
	}

	// NOTE: We don't run full-table copies in parallel. These are meant for
	// small-ish tables and should be kept short (due to full-table locking
	// on the source). For the same reason, we also don't "steal" one of the
	// limited goroutines controlled by d.Concurrency - we assume the below
	// goroutine completes fast whereas the above one could take a long time.
	go func() {
		defer wg.Done()

		logger := d.logger.WithField("copy-instance", "unpaginated")
		for {
			table, ok := <-unpaginatedTablesQueue
			if !ok {
				break
			}

			tableLogger := logger.WithField("table", table.String())
			tableLogger.Info("starting to process table")

			err := d.processUnpaginatedTable(table)
			if err == nil {
				tableLogger.Info("done processing table")
			} else if d.failOnFirstCopyError {
				d.ErrorHandler.Fatal("data_iterator", err)
			} else {
				tableLogger.Warn("suspending error until all table copies have been attempted")
				lastError = err
			}
		}

		logger.Info("copier shutting down")
	}()

	i := 0
	totalTablesToCopy := len(paginatedTables) + len(unpaginatedTables)
	loggingIncrement := totalTablesToCopy / 50
	if loggingIncrement == 0 {
		loggingIncrement = 1
	}

	for table, _ := range paginatedTables {
		paginatedTablesQueue <- table
		i++
		if i%loggingIncrement == 0 {
			d.logger.WithField("table", table.String()).Infof("queued table for paginated processing (%d/%d)", i, totalTablesToCopy)
		}
	}

	for _, table := range unpaginatedTables {
		unpaginatedTablesQueue <- table
		i++
		if i%loggingIncrement == 0 {
			d.logger.WithField("table", table.String()).Infof("queued table for full-table processing (%d/%d)", i, totalTablesToCopy)
		}
	}

	d.logger.Info("done queueing tables to be iterated, closing table channel")
	close(paginatedTablesQueue)
	close(unpaginatedTablesQueue)

	d.logger.Debug("waiting for table copy to complete")
	wg.Wait()

	// if we have delayed
	if lastError != nil {
		d.logger.Warn("table copy has queued up errors, re-raising now that all tables have been attempted")
		d.ErrorHandler.Fatal("data_iterator", lastError)
	}

	d.logger.Debug("table copy completed, notifying listeners")
	for _, listener := range d.doneListeners {
		listener()
	}
	d.logger.Debug("table copy done")
}

func (d *DataIterator) processPaginatedTable(table *TableSchema) error {
	logger := d.logger.WithField("table", table.String())

	targetPaginationKeyDataInterface, found := d.targetPaginationKeys.Load(table.String())
	if !found {
		err := fmt.Errorf("%s not found in targetPaginationKeys, this is likely a programmer error", table.String())
		logger.WithError(err).Error("this is definitely a bug")
		return err
	}
	targetPaginationKeyData := targetPaginationKeyDataInterface.(*PaginationKeyData)

	startPaginationKeyData, completed := d.StateTracker.LastSuccessfulPaginationKey(table.String())
	if completed {
		err := fmt.Errorf("%v has been marked as completed but a table iterator has been spawned, this is likely a programmer error which resulted in the inconsistent starting state", table.String())
		logger.WithError(err).Error("this is definitely a bug")
		return err
	}

	var cursor *PaginatedCursor
	if d.lockStrategy == LockTypeSourceDB {
		cursor = d.CursorConfig.NewPaginatedCursor(table, startPaginationKeyData, targetPaginationKeyData)
	} else {
		var tableLock *sync.RWMutex
		if d.lockStrategy == LockTypeInGhostferry {
			tableLock = d.StateTracker.GetTableLock(table.String())
		}
		cursor = d.CursorConfig.NewPaginatedCursorWithoutRowLock(table, startPaginationKeyData, targetPaginationKeyData, tableLock)
	}
	if d.SelectFingerprint {
		if len(cursor.ColumnsToSelect) == 0 {
			cursor.ColumnsToSelect = []string{"*"}
		}

		cursor.ColumnsToSelect = append(cursor.ColumnsToSelect, table.RowMd5Query())
	}

	err := cursor.Each(func(batch RowBatch) error {
		metrics.Count("RowEvent", int64(batch.Size()), []MetricTag{
			MetricTag{"table", table.Name},
			MetricTag{"source", "table"},
		}, 1.0)

		if d.SelectFingerprint {
			if insertRowBatch, ok := batch.(InsertRowBatch); ok {
				fingerprints := make(map[uint64][]byte)
				rows := make([]RowData, batch.Size())

				for i, rowData := range insertRowBatch.Values() {
					paginationKey, err := insertRowBatch.VerifierPaginationKey(i)
					if err != nil {
						logger.WithError(err).Error("failed to get paginationKey data")
						return err
					}

					fingerprints[paginationKey] = rowData[len(rowData)-1].([]byte)
					rows[i] = rowData[:len(rowData)-1]
				}

				batch = &DataRowBatch{
					values:       rows,
					table:        table,
					fingerprints: fingerprints,
				}
			}
		}

		for _, listener := range d.batchListeners {
			err := listener(batch)
			if err != nil {
				logger.WithError(err).Error("failed to process row batch with listeners")
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	logger.Debug("table iteration completed")
	return nil
}

func (d *DataIterator) processUnpaginatedTable(table *TableSchema) error {
	logger := d.logger.WithField("table", table.String())
	logger.Debug("Starting full-table copy")

	var tableLock *sync.RWMutex
	if d.lockStrategy == LockTypeInGhostferry {
		tableLock = d.StateTracker.GetTableLock(table.String())
	}
	cursor := d.CursorConfig.NewFullTableCursor(table, d.lockStrategy == LockTypeSourceDB, tableLock)

	err := cursor.Each(func(batch RowBatch) error {
		metrics.Count("RowEvent", int64(batch.Size()), []MetricTag{
			MetricTag{"table", table.Name},
			MetricTag{"source", "table"},
		}, 1.0)

		for _, listener := range d.batchListeners {
			err := listener(batch)
			if err != nil {
				logger.WithError(err).Error("failed to process full-table row batch with listeners")
				return err
			}
		}

		return nil
	})

	if err != nil {
		logger.WithError(err).Error("failed to scan full table")
		return err
	}

	logger.Debug("full table scan completed")
	return nil
}

func (d *DataIterator) AddBatchListener(listener func(RowBatch) error) {
	d.batchListeners = append(d.batchListeners, listener)
}

func (d *DataIterator) AddDoneListener(listener func() error) {
	d.doneListeners = append(d.doneListeners, listener)
}
