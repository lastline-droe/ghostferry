package test

import (
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/stretchr/testify/suite"
)

type DMLEventsTestSuite struct {
	suite.Suite

	tableMapEvent    *replication.TableMapEvent
	tableSchemaCache ghostferry.TableSchemaCache
	sourceTable      *ghostferry.TableSchema
	targetTable      *ghostferry.TableSchema
}

func (this *DMLEventsTestSuite) SetupTest() {
	this.tableMapEvent = &replication.TableMapEvent{
		Schema: []byte("test_schema"),
		Table:  []byte("test_table"),
	}

	columns := []schema.TableColumn{
		{Name: "col1", Type: schema.TYPE_NUMBER},
		{Name: "col2"},
		{Name: "col3"},
	}

	this.sourceTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "test_schema",
			Name:    "test_table",
			Columns: columns,
		},
		PaginationKey: &ghostferry.PaginationKey{[]*schema.TableColumn{&columns[0]}, []int{0}, 0},
	}

	this.targetTable = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:  "target_schema",
			Name:    "target_table",
			Columns: columns,
		},
		PaginationKey: &ghostferry.PaginationKey{[]*schema.TableColumn{&columns[0]}, []int{0}, 0},
	}

	this.tableSchemaCache = map[string]*ghostferry.TableSchema{
		"test_schema.test_table": this.sourceTable,
	}
}

func (this *DMLEventsTestSuite) TestBinlogInsertEventGeneratesInsertQuery() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), true},
			{1001, []byte("val2"), false},
		},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogInsertEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(2, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)
	this.Require().Equal(dmlEvents[1].EventTime(), now)
	this.Require().False(dmlEvents[0].IsAutoTransaction())
	this.Require().False(dmlEvents[1].IsAutoTransaction())

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`,`col3`) VALUES (1000,_binary'val1',1)", q1)

	q2, err := dmlEvents[1].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("INSERT IGNORE INTO `target_schema`.`target_table` (`col1`,`col2`,`col3`) VALUES (1001,_binary'val2',0)", q2)
}

func (this *DMLEventsTestSuite) TestBinlogInsertEventWithWrongColumnsReturnsError() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogInsertEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)

	_, err = dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestBinlogInsertEventMetadata() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogInsertEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)
	this.Require().Equal("test_schema", dmlEvents[0].Database())
	this.Require().Equal("test_table", dmlEvents[0].Table())
	this.Require().Nil(dmlEvents[0].OldValues())
	this.Require().Equal(ghostferry.RowData{1000}, dmlEvents[0].NewValues())
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventGeneratesUpdateQuery() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), true},
			{1000, []byte("val2"), false},
			{1001, []byte("val3"), false},
			{1001, []byte("val4"), true},
		},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogUpdateEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(2, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)
	this.Require().Equal(dmlEvents[1].EventTime(), now)
	this.Require().False(dmlEvents[0].IsAutoTransaction())
	this.Require().False(dmlEvents[1].IsAutoTransaction())

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("UPDATE `target_schema`.`target_table` SET `col1`=1000,`col2`=_binary'val2',`col3`=0 WHERE `col1`=1000 AND `col2`=_binary'val1' AND `col3`=1", q1)

	q2, err := dmlEvents[1].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("UPDATE `target_schema`.`target_table` SET `col1`=1001,`col2`=_binary'val4',`col3`=1 WHERE `col1`=1001 AND `col2`=_binary'val3' AND `col3`=0", q2)
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventWithWrongColumnsReturnsError() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}, {1000}},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogUpdateEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)

	_, err = dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventWithNull() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), nil},
			{1000, []byte("val2"), nil},
		},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogUpdateEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("UPDATE `target_schema`.`target_table` SET `col1`=1000,`col2`=_binary'val2',`col3`=NULL WHERE `col1`=1000 AND `col2`=_binary'val1' AND `col3` IS NULL", q1)
}

func (this *DMLEventsTestSuite) TestBinlogUpdateEventMetadata() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}, {1001}},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogUpdateEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)
	this.Require().Equal("test_schema", dmlEvents[0].Database())
	this.Require().Equal("test_table", dmlEvents[0].Table())
	this.Require().Equal(ghostferry.RowData{1000}, dmlEvents[0].OldValues())
	this.Require().Equal(ghostferry.RowData{1001}, dmlEvents[0].NewValues())
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventGeneratesDeleteQuery() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), true},
			{1001, []byte("val2"), false},
		},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogDeleteEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(2, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)
	this.Require().Equal(dmlEvents[1].EventTime(), now)
	this.Require().False(dmlEvents[0].IsAutoTransaction())
	this.Require().False(dmlEvents[1].IsAutoTransaction())

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("DELETE FROM `target_schema`.`target_table` WHERE `col1`=1000 AND `col2`=_binary'val1' AND `col3`=1", q1)

	q2, err := dmlEvents[1].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("DELETE FROM `target_schema`.`target_table` WHERE `col1`=1001 AND `col2`=_binary'val2' AND `col3`=0", q2)
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventWithNull() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows: [][]interface{}{
			{1000, []byte("val1"), nil},
		},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogDeleteEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)

	q1, err := dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().Nil(err)
	this.Require().Equal("DELETE FROM `target_schema`.`target_table` WHERE `col1`=1000 AND `col2`=_binary'val1' AND `col3` IS NULL", q1)
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventWithWrongColumnsReturnsError() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogDeleteEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)

	_, err = dmlEvents[0].AsSQLString(this.targetTable.Schema, this.targetTable.Name)
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "test_table has 3 columns but event has 1 column")
}

func (this *DMLEventsTestSuite) TestBinlogDeleteEventMetadata() {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  [][]interface{}{{1000}},
	}

	now := time.Now()
	dmlEvents, err := ghostferry.NewBinlogDeleteEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(1, len(dmlEvents))
	this.Require().Equal(dmlEvents[0].EventTime(), now)
	this.Require().Equal("test_schema", dmlEvents[0].Database())
	this.Require().Equal("test_table", dmlEvents[0].Table())
	this.Require().Equal(ghostferry.RowData{1000}, dmlEvents[0].OldValues())
	this.Require().Nil(dmlEvents[0].NewValues())
}

func (this *DMLEventsTestSuite) testPaginationKey(rows [][]interface{}) uint64 {
	rowsEvent := &replication.RowsEvent{
		Table: this.tableMapEvent,
		Rows:  rows,
	}
	dmlEvents, _ := ghostferry.NewBinlogInsertEvents(this.sourceTable, rowsEvent, ghostferry.BinlogPosition{}, time.Now())

	paginationKey, err := dmlEvents[0].VerifierPaginationKey()
	this.Require().Nil(err)
	this.Require().NotNil(paginationKey)
	return paginationKey
}

func (this *DMLEventsTestSuite) TestPaginationKeyForInt() {
	testValue := 1000
	paginationKey := this.testPaginationKey([][]interface{}{{testValue, 0, 0}})
	this.Require().Equal(paginationKey, uint64(testValue))
}

func (this *DMLEventsTestSuite) TestPaginationKeyForUint64() {
	testValue := uint64(1000)
	paginationKey := this.testPaginationKey([][]interface{}{{testValue, 0, 0}})
	this.Require().Equal(paginationKey, testValue)
}

func (this *DMLEventsTestSuite) TestPaginationKeyForUint32() {
	testValue := uint32(1000)
	paginationKey := this.testPaginationKey([][]interface{}{{testValue, 0, 0}})
	this.Require().Equal(paginationKey, uint64(testValue))
}

func (this *DMLEventsTestSuite) TestPaginationKeyForUint16() {
	testValue := uint16(1000)
	paginationKey := this.testPaginationKey([][]interface{}{{testValue, 0, 0}})
	this.Require().Equal(paginationKey, uint64(testValue))
}

func (this *DMLEventsTestSuite) TestPaginationKeyForUint8() {
	testValue := uint8(255)
	paginationKey := this.testPaginationKey([][]interface{}{{testValue, 0, 0}})
	this.Require().Equal(paginationKey, uint64(testValue))
}

func (this *DMLEventsTestSuite) TestPaginationKeyForUint() {
	testValue := uint(1000)
	paginationKey := this.testPaginationKey([][]interface{}{{testValue, 0, 0}})
	this.Require().Equal(paginationKey, uint64(testValue))
}

func (this *DMLEventsTestSuite) TestPaginationKeyForByteArray() {
	paginationKey := this.testPaginationKey([][]interface{}{{[]byte("1000"), 0, 0}})
	this.Require().Equal(paginationKey, uint64(1000))
}

func (this *DMLEventsTestSuite) TestPaginationKeyForString() {
	paginationKey := this.testPaginationKey([][]interface{}{{"1000", 0, 0}})
	this.Require().Equal(paginationKey, uint64(1000))
}

func TestDMLEventsTestSuite(t *testing.T) {
	suite.Run(t, new(DMLEventsTestSuite))
}

type DDLEventsTestSuite struct {
	suite.Suite
}

func (this *DDLEventsTestSuite) TestBinlogQueryGeneratesDDLEvent() {
	now := time.Now()
	ddlStatement := "DELETE TABLE testdb.testtable"
	affectedTable := ghostferry.NewQualifiedTableName("testdb", "testtable")
	ddlEvent, err := ghostferry.NewBinlogDDLEvent(ddlStatement, &affectedTable, ghostferry.BinlogPosition{}, now)
	this.Require().Nil(err)
	this.Require().Equal(ddlEvent.EventTime(), now)
	this.Require().Equal(ddlEvent.Database(), "testdb")
	this.Require().Equal(ddlEvent.Table(), "testtable")
	this.Require().True(ddlEvent.IsAutoTransaction())

	q, err := ddlEvent.AsSQLString("testdb", "testtable")
	this.Require().Nil(err)
	this.Require().Equal("USE `testdb`;\n" + ddlStatement, q)
}

func (this *DDLEventsTestSuite) TestBinlogQueryWithDBOrTableRenameGeneratesDDLEventError() {
	ddlStatement := "DELETE TABLE testdb.testtable"
	affectedTable := ghostferry.NewQualifiedTableName("testdb", "testtable")
	ddlEvent, err := ghostferry.NewBinlogDDLEvent(ddlStatement, &affectedTable, ghostferry.BinlogPosition{}, time.Now())
	this.Require().Nil(err)

	_, err1 := ddlEvent.AsSQLString("renameddb", "testtable")
	this.Require().Contains(err1.Error(), "cannot use remapped schema/tableName renameddb.testtable for migration of testdb.testtable")

	_, err2 := ddlEvent.AsSQLString("testdb", "renamedtable")
	this.Require().Contains(err2.Error(), "cannot use remapped schema/tableName testdb.renamedtable for migration of testdb.testtable")
}

func TestDDLEventsTestSuite(t *testing.T) {
	suite.Run(t, new(DDLEventsTestSuite))
}
