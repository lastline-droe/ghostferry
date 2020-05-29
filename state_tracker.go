package ghostferry

import (
	"container/ring"
	"encoding/json"
	"fmt"
	sqlorig "database/sql"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/sirupsen/logrus"
)

// StateTracker design
// ===================
//
// General Overview
// ----------------
//
// The state tracker keeps track of the progress of Ghostferry so it can be
// interrupted and resumed. The state tracker is supposed to be initialized and
// managed by the Ferry. Each Ghostferry components, such as the `BatchWriter`,
// will get passed an instance of the StateTracker. During the run, the
// components will update their last successful components to the state tracker
// instance given via the state tracker API defined here.
//
// The states stored in the state tracker can be copied into a
// serialization-friendly struct (`SerializableState`), which can then be
// dumped using something like JSON. Assuming the rest of Ghostferry used the
// API of the state tracker correctlym this can be done at any point during the
// Ghostferry run and the resulting state can be resumed from without data
// loss.  The same `SerializableState` is used as an input to `Ferry`, which
// will instruct the `Ferry` to resume a previously interrupted run.

type SerializableState struct {
	GhostferryVersion         string
	LastKnownTableSchemaCache TableSchemaCache

	LastSuccessfulPaginationKeys              map[string]*PaginationKeyData
	CompletedTables                           map[string]bool
	LastWrittenBinlogPosition                 BinlogPosition
	LastStoredBinlogPositionForInlineVerifier BinlogPosition
	BinlogVerifyStore                         BinlogVerifySerializedStore
}

func (s *SerializableState) MinBinlogPosition() BinlogPosition {
	nilPosition := BinlogPosition{}
	if s.LastWrittenBinlogPosition == nilPosition {
		return s.LastStoredBinlogPositionForInlineVerifier
	}

	if s.LastStoredBinlogPositionForInlineVerifier == nilPosition {
		return s.LastWrittenBinlogPosition
	}

	if s.LastWrittenBinlogPosition.Compare(s.LastStoredBinlogPositionForInlineVerifier) >= 0 {
		return s.LastStoredBinlogPositionForInlineVerifier
	} else {
		return s.LastWrittenBinlogPosition
	}
}

// For tracking the speed of the copy
type PaginationKeyPositionLog struct {
	Position uint64
	At       time.Time
}

func newSpeedLogRing(speedLogCount int) *ring.Ring {
	if speedLogCount <= 0 {
		return nil
	}

	speedLog := ring.New(speedLogCount)
	speedLog.Value = PaginationKeyPositionLog{
		Position: 0,
		At:       time.Now(),
	}

	return speedLog
}

type StateTracker struct {
	BinlogRWMutex *sync.RWMutex
	CopyRWMutex   *sync.RWMutex

	lastWrittenBinlogPosition                 BinlogPosition
	lastStoredBinlogPositionForInlineVerifier BinlogPosition

	lastSuccessfulPaginationKeys map[string]*PaginationKeyData
	completedTables              map[string]bool
	tableLocks                   map[string]*sync.RWMutex

	// optional database+table prefix to which we write the current status
	stateTablesPrefix string

	logger            *logrus.Entry
	iterationSpeedLog *ring.Ring
}

func NewStateTracker(speedLogCount int) *StateTracker {
	return &StateTracker{
		BinlogRWMutex: &sync.RWMutex{},
		CopyRWMutex:   &sync.RWMutex{},

		lastSuccessfulPaginationKeys: make(map[string]*PaginationKeyData),
		completedTables:              make(map[string]bool),
		tableLocks:                   make(map[string]*sync.RWMutex),
		logger:                       logrus.WithField("tag", "state_tracker"),
		iterationSpeedLog:            newSpeedLogRing(speedLogCount),
	}
}

// serializedState is a state the tracker should start from, as opposed to
// starting from the beginning.
func NewStateTrackerFromSerializedState(speedLogCount int, serializedState *SerializableState, tables TableSchemaCache) (*StateTracker, error) {
	s := NewStateTracker(speedLogCount)
	s.lastSuccessfulPaginationKeys = serializedState.LastSuccessfulPaginationKeys
	s.completedTables = serializedState.CompletedTables
	s.lastWrittenBinlogPosition = serializedState.LastWrittenBinlogPosition
	s.lastStoredBinlogPositionForInlineVerifier = serializedState.LastStoredBinlogPositionForInlineVerifier

	for tableName, paginationKeyData := range s.lastSuccessfulPaginationKeys {
		table := tables[tableName]
		if table == nil {
			return nil, fmt.Errorf("resume state contains pagination data for unknown table %s", tableName)
		}

		unmarshalledPaginationKeyData, err := UnmarshalPaginationKeyData(paginationKeyData, table)
		if err != nil {
			return nil, err
		}
		s.lastSuccessfulPaginationKeys[tableName] = unmarshalledPaginationKeyData
	}

	return s, nil
}

func NewStateTrackerFromTargetDB(f *Ferry) (s *StateTracker, state *SerializableState, err error) {
	s = NewStateTracker(f.DataIterationConcurrency*10)
	s.stateTablesPrefix = fmt.Sprintf("%s._ghostferry_%d_", f.Config.ResumeStateFromDB, f.MyServerId)

	state, err = s.readStateFromDB(f)
	if err == nil && state == nil {
		err = s.initializeDBStateSchema(f.TargetDB, f.Config.ResumeStateFromDB)

		s.logger.Debug("initializing resume state from binlog position on source DB")
		masterPos, posErr := ShowMasterStatusBinlogPosition(f.SourceDB)
		if posErr != nil {
			s.logger.WithError(posErr).Error("failed to read current binlog position")
			err = posErr
			return
		}

		pos := NewResumableBinlogPosition(masterPos)
		s.logger.Debugf("using resume state from binlog position on source DB: %s", pos)
		s.UpdateLastWrittenBinlogPosition(pos)
		s.UpdateLastStoredBinlogPositionForInlineVerifier(pos)
		// we absolutely need to initialize the DB with a proper state of the source
		// DB here, or we may end up never writing the state to the target DB state
		// tables, meaning that we resume at an invalid position although we already
		// started copying table rows
		s.SerializeToDB(f.TargetDB)
	}

	return
}

func (s *StateTracker) UpdateLastWrittenBinlogPosition(pos BinlogPosition) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.logger.Debugf("updating last written binlog position: %s", pos)
	s.lastWrittenBinlogPosition = pos
}

func (s *StateTracker) UpdateLastStoredBinlogPositionForInlineVerifier(pos BinlogPosition) {
	s.BinlogRWMutex.Lock()
	defer s.BinlogRWMutex.Unlock()

	s.logger.Debugf("updating stored binlog position for inline verifier: %s", pos)
	s.lastStoredBinlogPositionForInlineVerifier = pos
}

func (s *StateTracker) UpdateLastSuccessfulPaginationKey(table string, paginationKey *PaginationKeyData) {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	s.logger.WithField("table", table).Debugf("updating table last successful pagination key")
	if IncrediblyVerboseLogging {
		s.logger.WithField("table", table).Debugf("updating table last successful pagination key: %s", paginationKey)
	}

	var deltaPaginationKey uint64
	if s.lastSuccessfulPaginationKeys[table] != nil {
		if progress, ok := paginationKey.ProgressData(); ok {
			if base, ok := s.lastSuccessfulPaginationKeys[table].ProgressData(); ok {
				deltaPaginationKey = progress - base
			}
		}
	}
	s.lastSuccessfulPaginationKeys[table] = paginationKey

	s.updateSpeedLog(deltaPaginationKey)
}

func (s *StateTracker) LastSuccessfulPaginationKey(table string) (paginationKeyData *PaginationKeyData, completed bool) {
	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	_, found := s.completedTables[table]
	if found {
		return nil, true
	}

	paginationKey, _ := s.lastSuccessfulPaginationKeys[table]
	return paginationKey, false
}

func (s *StateTracker) MarkTableAsCompleted(table string) {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	s.logger.WithField("table", table).Debug("marking table as completed")
	s.completedTables[table] = true
}

func (s *StateTracker) IsTableComplete(table string) bool {
	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	return s.completedTables[table]
}

func (s *StateTracker) GetTableLock(table string) *sync.RWMutex {
	s.CopyRWMutex.Lock()
	defer s.CopyRWMutex.Unlock()

	// table locks are needed only for synchronizing data copy and binlog
	// writing. We optimize this into a NULL-lock if we know this race is
	// not possible
	if s.completedTables[table] {
		return nil
	}

	if lock, found := s.tableLocks[table]; found {
		return lock
	}

	lock := &sync.RWMutex{}
	s.tableLocks[table] = lock
	return lock
}

// This is reasonably accurate if the rows copied are distributed uniformly
// between paginationKey = 0 -> max(paginationKey). It would not be accurate if the distribution is
// concentrated in a particular region.
func (s *StateTracker) EstimatedPaginationKeysPerSecond() float64 {
	if s.iterationSpeedLog == nil {
		return 0.0
	}

	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	if s.iterationSpeedLog.Value.(PaginationKeyPositionLog).Position == 0 {
		return 0.0
	}

	earliest := s.iterationSpeedLog
	for earliest.Prev() != nil && earliest.Prev() != s.iterationSpeedLog && earliest.Prev().Value.(PaginationKeyPositionLog).Position != 0 {
		earliest = earliest.Prev()
	}

	currentValue := s.iterationSpeedLog.Value.(PaginationKeyPositionLog)
	earliestValue := earliest.Value.(PaginationKeyPositionLog)
	deltaPaginationKey := currentValue.Position - earliestValue.Position
	deltaT := currentValue.At.Sub(earliestValue.At).Seconds()

	return float64(deltaPaginationKey) / deltaT
}

func (s *StateTracker) updateSpeedLog(deltaPaginationKey uint64) {
	if s.iterationSpeedLog == nil {
		return
	}

	currentTotalPaginationKey := s.iterationSpeedLog.Value.(PaginationKeyPositionLog).Position
	s.iterationSpeedLog = s.iterationSpeedLog.Next()
	s.iterationSpeedLog.Value = PaginationKeyPositionLog{
		Position: currentTotalPaginationKey + deltaPaginationKey,
		At:       time.Now(),
	}
}

func (s *StateTracker) Serialize(lastKnownTableSchemaCache TableSchemaCache, binlogVerifyStore *BinlogVerifyStore) *SerializableState {
	s.BinlogRWMutex.RLock()
	defer s.BinlogRWMutex.RUnlock()

	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	state := &SerializableState{
		GhostferryVersion:                         VersionString,
		LastKnownTableSchemaCache:                 lastKnownTableSchemaCache,
		LastSuccessfulPaginationKeys:              make(map[string]*PaginationKeyData),
		CompletedTables:                           make(map[string]bool),
		LastWrittenBinlogPosition:                 s.lastWrittenBinlogPosition,
		LastStoredBinlogPositionForInlineVerifier: s.lastStoredBinlogPositionForInlineVerifier,
	}

	if binlogVerifyStore != nil {
		state.BinlogVerifyStore = binlogVerifyStore.Serialize()
	}

	// Need a copy because lastSuccessfulPaginationKeys may change after Serialize
	// returns. This would inaccurately reflect the state of Ghostferry when
	// Serialize is called.
	for k, v := range s.lastSuccessfulPaginationKeys {
		state.LastSuccessfulPaginationKeys[k] = v
	}

	for k, v := range s.completedTables {
		state.CompletedTables[k] = v
	}

	return state
}

func (s *StateTracker) SerializeToDB(db *sql.DB) error {
	if s.stateTablesPrefix == "" {
		return nil
	}

	s.BinlogRWMutex.RLock()
	defer s.BinlogRWMutex.RUnlock()

	s.CopyRWMutex.RLock()
	defer s.CopyRWMutex.RUnlock()

	binlogTableName := s.getBinLogWriterStateTable()
	s.logger.Debugf("storing state table %s: %v", binlogTableName, s.lastWrittenBinlogPosition)
	binlogInitSql, binlogInitArgs, err := s.GetStoreBinlogWriterPositionSql(s.lastWrittenBinlogPosition, time.Unix(1 /* unix(0) is not a valid timestamp in MySQL */, 0))
	if err != nil {
		s.logger.WithField("err", err).Errorf("generating state sql for %s failed", binlogTableName)
		return err
	}
	_, err = db.Exec(binlogInitSql, binlogInitArgs...)
	if err != nil {
		s.logger.WithField("err", err).Errorf("storing state table %s failed", binlogTableName)
		return err
	}

	inlineVerifierTableName := s.getInlineVerifierStateTable()
	s.logger.Debugf("storing state table %s: %v", inlineVerifierTableName, s.lastStoredBinlogPositionForInlineVerifier)
	inlineVerifierInitSql, inlineVerifierInitArgs, err := s.GetStoreInlineVerifierPositionSql(s.lastStoredBinlogPositionForInlineVerifier)
	if err != nil {
		s.logger.WithField("err", err).Errorf("generating state sql for %s failed", inlineVerifierTableName)
		return err
	}
	_, err = db.Exec(inlineVerifierInitSql, inlineVerifierInitArgs...)
	if err != nil {
		s.logger.WithField("err", err).Errorf("storing state table %s failed", inlineVerifierTableName)
		return err
	}

	for tableName, lastPaginationKey := range s.lastSuccessfulPaginationKeys {
		s.logger.Debugf("storing copy state for %s: %s", tableName, lastPaginationKey)

		paginationSql, paginationArgs, err := s.GetStoreRowCopyPositionSql(tableName, lastPaginationKey)
		if err != nil {
			s.logger.WithField("err", err).Errorf("generating copy-state sql for %s failed", tableName)
			return err
		}
		_, err = db.Exec(paginationSql, paginationArgs...)
		if err != nil {
			s.logger.WithField("err", err).Errorf("storing copy-state for %s failed", tableName)
			return err
		}
	}

	for tableName, isDone := range s.completedTables {
		if isDone {
			s.logger.Debugf("storing copy state done for %s", tableName)

			doneSql, doneArgs, err := s.GetStoreRowCopyDoneSql(tableName)
			if err != nil {
				s.logger.WithField("err", err).Errorf("generating copy-state-done sql for %s failed", tableName)
				return err
			}
			_, err = db.Exec(doneSql, doneArgs...)
			if err != nil {
				s.logger.WithField("err", err).Errorf("storing copy-state-done for %s failed", tableName)
				return err
			}
		}
	}

	return nil
}

func (s *StateTracker) getRowCopyStateTable() string {
	return s.stateTablesPrefix + "_row_copy_state"
}

func (s *StateTracker) getBinLogWriterStateTable() string {
	return s.stateTablesPrefix + "_last_binlog_writer_state"
}

func (s *StateTracker) getInlineVerifierStateTable() string {
	return s.stateTablesPrefix + "_last_inline_verifier_state"
}

func (s *StateTracker) initializeDBStateSchema(db *sql.DB, stateDatabase string) error {
	s.logger.Infof("initializing resume data target database")

	createDatabaseQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", stateDatabase)
	s.logger.Debugf("creating state database %s on target", stateDatabase)
	_, err := db.Exec(createDatabaseQuery)
	if err != nil {
		s.logger.WithField("err", err).Errorf("creating state database %s on target failed", s.stateTablesPrefix)
		return err
	}

	rowCopyTableName := s.getRowCopyStateTable()
	rowCopyCreateTable := `
CREATE TABLE ` + rowCopyTableName + ` (
    table_name varchar(255) CHARACTER SET ascii NOT NULL,
    last_pagination_key TEXT NOT NULL,
    copy_complete BOOLEAN NOT NULL DEFAULT FALSE,
    last_write_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name)
)`
	s.logger.Debugf("creating state table %s on target", rowCopyTableName)
	_, err = db.Exec(rowCopyCreateTable)
	if err != nil {
		s.logger.WithField("err", err).Errorf("creating state table %s on target failed", rowCopyTableName)
		return err
	}

	binlogWriterTableName := s.getBinLogWriterStateTable()
	binlogWriterCreateTable := `
CREATE TABLE ` + binlogWriterTableName + ` (
    event_filename varchar(255) CHARACTER SET ascii NOT NULL,
    event_pos int(11) UNSIGNED NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    resume_filename varchar(255) CHARACTER SET ascii NOT NULL,
    resume_pos int(11) UNSIGNED NOT NULL,
    write_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
INSERT INTO ` + binlogWriterTableName + ` (event_filename, event_pos, event_timestamp, resume_filename, resume_pos)
    VALUES ('', 0, FROM_UNIXTIME(1), '', 0)
`
	s.logger.Debugf("creating state table %s on target", binlogWriterTableName)
	_, err = db.Exec(binlogWriterCreateTable)
	if err != nil {
		s.logger.WithField("err", err).Errorf("creating state table %s on target failed", binlogWriterTableName)
		return err
	}

	// NOTE: The binlog-writer and inline-verifier state tables are very likely
	// to diverge in state shortly - that's why the code seems a bit repetitive,
	// but soon won't be so similar any longer
	inlineVerifierTableName := s.getInlineVerifierStateTable()
	inlineVerifierCreateTable := `
CREATE TABLE ` + inlineVerifierTableName + ` (
    event_filename varchar(255) CHARACTER SET ascii NOT NULL,
    event_pos int(11) UNSIGNED NOT NULL,
    resume_filename varchar(255) CHARACTER SET ascii NOT NULL,
    resume_pos int(11) UNSIGNED NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
INSERT INTO ` + inlineVerifierTableName + ` (event_filename, event_pos, resume_filename, resume_pos)
    VALUES ('', 0, '', 0)
`
	s.logger.Debugf("creating state table %s on target", inlineVerifierTableName)
	_, err = db.Exec(inlineVerifierCreateTable)
	if err != nil {
		s.logger.WithField("err", err).Errorf("creating state table %s on target failed", inlineVerifierTableName)
		return err
	}

	return nil
}

func (s *StateTracker) readStateFromDB(f *Ferry) (*SerializableState, error) {
	tokens := strings.Split(s.getRowCopyStateTable(), ".")
	if len(tokens) != 2 {
		return nil, fmt.Errorf("invalid target DB state table name %v", tokens)
	}

	query, args, err := squirrel.
		Select("1").
		From("information_schema.tables").
		Where(squirrel.Eq{"table_schema": tokens[0], "table_name": tokens[1]}).
		ToSql()
	if err != nil {
		s.logger.WithField("err", err).Errorf("reading target DB tables failed")
		return nil, err
	}

	var dummy uint64
	err = f.TargetDB.QueryRow(query, args...).Scan(&dummy)
	if err == sqlorig.ErrNoRows {
		return nil, nil
	}

	s.logger.Infof("reading resume data from target database")

	state := &SerializableState{
		GhostferryVersion:            VersionString,
		LastKnownTableSchemaCache:    f.Tables,
		LastSuccessfulPaginationKeys: make(map[string]*PaginationKeyData),
		CompletedTables:              make(map[string]bool),
	}

	rowCopyTableName := s.getRowCopyStateTable()
	s.logger.Debugf("reading state table %s from target", rowCopyTableName)
	rowCopyRows, err := squirrel.
		Select("table_name", "last_pagination_key", "copy_complete").
		From(rowCopyTableName).
		RunWith(f.TargetDB.DB).
		Query()
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"err": err,
			"table": rowCopyTableName,
		}).Errorf("reading row-copy resume data from target DB failed")
		return nil, err
	}
	defer rowCopyRows.Close()

	for rowCopyRows.Next() {
		var tableName string
		var lastPaginationKey string
		var copyComplete bool
		err = rowCopyRows.Scan(&tableName, &lastPaginationKey, &copyComplete)
		if err != nil {
			s.logger.WithFields(logrus.Fields{
				"err":   err,
				"table": rowCopyTableName,
			}).Errorf("parsing row-copy resume data row from target DB failed")
			return nil, err
		}

		logger := s.logger.WithField("table", tableName)
		// having the data that we tried to parse is incredibly useful
		// for debugging, but the data should be considered
		// confidential, so we cannot emit it to logs by default, even
		// in debug-mode
		if IncrediblyVerboseLogging {
			logger = logger.WithField("data", lastPaginationKey)
		}

		var table *TableSchema
		var ok bool
		if table, ok = f.Tables[tableName]; !ok {
			logger.Warningf("row-copy resume data contains state for unknown table")
			continue
		}

		// non-paginated tables don't have resume key data
		if lastPaginationKey != "" {
			var lastPaginationKeyData PaginationKeyData
			err = json.NewDecoder(strings.NewReader(lastPaginationKey)).Decode(&lastPaginationKeyData)
			if err != nil {
				logger.WithField("err", err).Errorf("parsing row-copy resume key from target DB failed")
				return nil, err
			}

			keyData, err := UnmarshalPaginationKeyData(&lastPaginationKeyData, table)
			if err != nil {
				logger.WithField("err", err).Errorf("unmarshalling row-copy resume key from target DB failed")
				return nil, err
			}

			state.LastSuccessfulPaginationKeys[tableName] = keyData
			s.UpdateLastSuccessfulPaginationKey(tableName, keyData)
		}
		if copyComplete {
			s.MarkTableAsCompleted(tableName)
			state.CompletedTables[tableName] = true
		}
	}

	binlogWriterTableName := s.getBinLogWriterStateTable()
	s.logger.Debugf("reading state table %s from target", binlogWriterTableName)
	binlogWriterRows, err := squirrel.
		Select("event_filename", "event_pos", "resume_filename", "resume_pos").
		From(binlogWriterTableName).
		Limit(1).
		RunWith(f.TargetDB.DB).
		Query()
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"err": err,
			"table": binlogWriterTableName,
		}).Errorf("reading binlog writer resume data from target DB failed")
		return nil, err
	}
	defer binlogWriterRows.Close()

	for binlogWriterRows.Next() {
		err = binlogWriterRows.Scan(&state.LastWrittenBinlogPosition.EventPosition.Name, &state.LastWrittenBinlogPosition.EventPosition.Pos, &state.LastWrittenBinlogPosition.ResumePosition.Name, &state.LastWrittenBinlogPosition.ResumePosition.Pos)
		if err != nil {
			s.logger.WithFields(logrus.Fields{
				"err":   err,
				"table": binlogWriterTableName,
			}).Errorf("parsing binlog writer resume data row from target DB failed")
			return nil, err
		}
		f.logger.Infof("found binlog writer resume position data on target DB: %s", state.LastWrittenBinlogPosition)
		s.UpdateLastWrittenBinlogPosition(state.LastWrittenBinlogPosition)
	}

	inlineVerifierTableName := s.getInlineVerifierStateTable()
	s.logger.Debugf("reading state table %s from target", inlineVerifierTableName)
	inlineVerifierRows, err := squirrel.
		Select("event_filename", "event_pos", "resume_filename", "resume_pos").
		From(inlineVerifierTableName).
		RunWith(f.TargetDB.DB).
		Limit(1).
		Query()
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"err": err,
			"table": inlineVerifierTableName,
		}).Errorf("reading inline-verifier resume data from target DB failed")
		return nil, err
	}
	defer inlineVerifierRows.Close()

	for inlineVerifierRows.Next() {
		err = inlineVerifierRows.Scan(&state.LastStoredBinlogPositionForInlineVerifier.EventPosition.Name, &state.LastStoredBinlogPositionForInlineVerifier.EventPosition.Pos, &state.LastStoredBinlogPositionForInlineVerifier.ResumePosition.Name, &state.LastStoredBinlogPositionForInlineVerifier.ResumePosition.Pos)
		if err != nil {
			s.logger.WithFields(logrus.Fields{
				"err":   err,
				"table": inlineVerifierTableName,
			}).Errorf("parsing inline-verifier resume position data row from target DB failed")
			return nil, err
		}
		f.logger.Infof("found inline-verifier resume position data on target DB: %s", state.LastStoredBinlogPositionForInlineVerifier)
		s.UpdateLastStoredBinlogPositionForInlineVerifier(state.LastStoredBinlogPositionForInlineVerifier)
	}

	return state, nil
}

func (s *StateTracker) GetStoreBinlogWriterPositionSql(pos BinlogPosition, lastEventTs time.Time) (sqlStr string, args []interface{}, err error) {
	if s.stateTablesPrefix == "" {
		return
	}

	// NOTE: It seems we cannot use a prepared statement here, because the
	// binlog writer builds a transaction manually. To make sure we don't have
	// any SQL-injection, we validate the string parameters manually and make
	// sure to print anything else as INTs
	if strings.Contains(pos.EventPosition.Name, "'") || strings.Contains(pos.ResumePosition.Name, "'") {
		err = fmt.Errorf("unexpected/invalid binlog position name: %s", pos)
		return
	}

	sqlStr = fmt.Sprintf(`
UPDATE %s
SET event_filename='%s', event_pos=%d, 
    event_timestamp=FROM_UNIXTIME(%d),
    resume_filename='%s', resume_pos=%d
`,
		s.getBinLogWriterStateTable(),
		pos.EventPosition.Name, pos.EventPosition.Pos,
		lastEventTs.Unix(),
		pos.ResumePosition.Name, pos.ResumePosition.Pos,
	)

	return
}

func (s *StateTracker) GetStoreInlineVerifierPositionSql(pos BinlogPosition) (sqlStr string, args []interface{}, err error) {
	if s.stateTablesPrefix == "" {
		return
	}

	sqlStr, args, err = squirrel.
		Update(s.getInlineVerifierStateTable()).
		Set("event_filename", pos.EventPosition.Name).
		Set("event_pos", pos.EventPosition.Pos).
		Set("resume_filename", pos.ResumePosition.Name).
		Set("resume_pos", pos.ResumePosition.Pos).
		ToSql()

	return
}

func (s *StateTracker) GetStoreRowCopyDoneSql(tableName string) (sqlStr string, args []interface{}, err error) {
	if s.stateTablesPrefix == "" {
		return
	}

	sqlStr, args, err = squirrel.
		Insert(s.getRowCopyStateTable()).
		Columns("table_name", "last_pagination_key", "copy_complete").
		Values(tableName, "", 1).
		Suffix("ON DUPLICATE KEY UPDATE copy_complete=1").
		ToSql()

	return
}

func (s *StateTracker) GetStoreRowCopyPositionSql(tableName string, endPaginationKey *PaginationKeyData) (sqlStr string, args []interface{}, err error) {
	if s.stateTablesPrefix == "" {
		return
	}

	paginationKeyData := ""
	if endPaginationKey != nil {
		stateBytes, err := json.Marshal(endPaginationKey)
		if err != nil {
			return "", nil, err
		}

		paginationKeyData = string(stateBytes)
	}
	sqlStr, args, err = squirrel.
		Insert(s.getRowCopyStateTable()).
		Columns("table_name", "last_pagination_key").
		Values(tableName, paginationKeyData).
		Suffix("ON DUPLICATE KEY UPDATE last_pagination_key=?", paginationKeyData).
		ToSql()

	return
}
