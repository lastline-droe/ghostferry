package test

import (
	"github.com/siddontang/go-mysql/schema"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/testhelpers"
)

type SimplePaginationKeyTestSuite struct {
	suite.Suite

	table *ghostferry.TableSchema
	rows []ghostferry.RowData
	columnsToSelect []string
}

func (this *SimplePaginationKeyTestSuite) SetupTest() {
	columns := []schema.TableColumn{
		schema.TableColumn{Name: "col1", Type: schema.TYPE_NUMBER},
		schema.TableColumn{Name: "col2", Type: schema.TYPE_STRING},
	}
	paginationKey := ghostferry.PaginationKey{
		Columns:       []*schema.TableColumn{&columns[0]},
		ColumnIndices: []int{0},
	}
	this.table = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:    "test_schema",
			Name:      "test_table",
			Columns:   columns,
			PKColumns: []int{0},
		},
		PaginationKey: &paginationKey,
	}
	this.rows = []ghostferry.RowData{ghostferry.RowData{1, "one"}, ghostferry.RowData{2, "two"}}
	this.columnsToSelect = []string{"*"}
}

func (this *SimplePaginationKeyTestSuite) TestDefaultBuildSelect() {
	builder := ghostferry.DefaultBuildSelect(this.columnsToSelect, this.table, nil, 5, false)
	sql, args, err := builder.ToSql()
	this.Require().Nil(err)
	this.Require().Equal(sql, "SELECT * FROM `test_schema`.`test_table` ORDER BY `col1` LIMIT 5")
	this.Require().Equal(len(args), 0)
}

func (this *SimplePaginationKeyTestSuite) TestDefaultBuildSelectDescending() {
	builder := ghostferry.DefaultBuildSelect(this.columnsToSelect, this.table, nil, 5, false)
	sql, args, err := builder.ToSql()
	this.Require().Nil(err)
	this.Require().Equal(sql, "SELECT * FROM `test_schema`.`test_table` ORDER BY `col1` DESC LIMIT 5")
	this.Require().Equal(len(args), 0)
}

func (this *SimplePaginationKeyTestSuite) TestDefaultBuildSelectWithResumeData() {
	lastPaginationKeyData, err := ghostferry.NewPaginationKeyDataFromRow(this.rows[0], this.table.PaginationKey)
	this.Require().Nil(err)

	builder := ghostferry.DefaultBuildSelect(this.columnsToSelect, this.table, lastPaginationKeyData, 5, false)
	sql, args, err := builder.ToSql()
	this.Require().Nil(err)
	this.Require().Equal(sql, "SELECT * FROM `test_schema`.`test_table` WHERE `col1`>? ORDER BY `col1` LIMIT 5")
	this.Require().Equal(args, []interface{}{int64(1)})
}

func (this *SimplePaginationKeyTestSuite) TestDefaultBuildSelectWithResumeDataDescending() {
	lastPaginationKeyData, err := ghostferry.NewPaginationKeyDataFromRow(this.rows[0], this.table.PaginationKey)
	this.Require().Nil(err)

	builder := ghostferry.DefaultBuildSelect(this.columnsToSelect, this.table, lastPaginationKeyData, 5, false)
	sql, args, err := builder.ToSql()
	this.Require().Nil(err)
	this.Require().Equal(sql, "SELECT * FROM `test_schema`.`test_table` WHERE `col1`<? ORDER BY `col1` DESC LIMIT 5")
	this.Require().Equal(args, []interface{}{int64(1)})
}

func (this *SimplePaginationKeyTestSuite) TestPaginationKeyDatacompare() {
	data1, err := ghostferry.NewPaginationKeyDataFromRow(this.rows[0], this.table.PaginationKey)
	this.Require().Nil(err)

	data2, err := ghostferry.NewPaginationKeyDataFromRow(this.rows[1], this.table.PaginationKey)
	this.Require().Nil(err)

	this.Require().Equal(data1.Compare(data1), 0)
	this.Require().Equal(data1.Compare(data2), -1)
	this.Require().Equal(data2.Compare(data1), 1)
}

func TestSimplePaginationKey(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, new(SimplePaginationKeyTestSuite))
}

type CompositePaginationKeyTestSuite struct {
	suite.Suite

	table *ghostferry.TableSchema
	rows []ghostferry.RowData
	columnsToSelect []string
}

func (this *CompositePaginationKeyTestSuite) SetupTest() {
	columns := []schema.TableColumn{
		schema.TableColumn{Name: "col1", Type: schema.TYPE_NUMBER},
		schema.TableColumn{Name: "col2", Type: schema.TYPE_STRING},
		schema.TableColumn{Name: "col3", Type: schema.TYPE_NUMBER},
		schema.TableColumn{Name: "col4", Type: schema.TYPE_FLOAT},
	}
	paginationKey := ghostferry.PaginationKey{
		Columns:       []*schema.TableColumn{&columns[0], &columns[1], &columns[2]},
		ColumnIndices: []int{0, 1, 2},
	}
	this.table = &ghostferry.TableSchema{
		Table: &schema.Table{
			Schema:    "test_schema",
			Name:      "test_table",
			Columns:   columns,
			PKColumns: []int{0, 1, 2},
		},
		PaginationKey: &paginationKey,
	}
	this.rows = []ghostferry.RowData{ghostferry.RowData{1, "one", 2, 3.4}, ghostferry.RowData{1, "two", 5, 6.7}}
	this.columnsToSelect = []string{"*"}
}

func (this *CompositePaginationKeyTestSuite) TestDefaultBuildSelect() {
	builder := ghostferry.DefaultBuildSelect(this.columnsToSelect, this.table, nil, 5, false)
	sql, args, err := builder.ToSql()
	this.Require().Nil(err)
	this.Require().Equal(sql, "SELECT * FROM `test_schema`.`test_table` ORDER BY `col1`, `col2`, `col3` LIMIT 5")
	this.Require().Equal(len(args), 0)
}

func (this *CompositePaginationKeyTestSuite) TestDefaultBuildSelectDescending() {
	builder := ghostferry.DefaultBuildSelect(this.columnsToSelect, this.table, nil, 5, false)
	sql, args, err := builder.ToSql()
	this.Require().Nil(err)
	this.Require().Equal(sql, "SELECT * FROM `test_schema`.`test_table` ORDER BY `col1` DESC, `col2` DESC, `col3` DESC LIMIT 5")
	this.Require().Equal(len(args), 0)
}

func (this *CompositePaginationKeyTestSuite) TestDefaultBuildSelectWithResumeData() {
	lastPaginationKeyData, err := ghostferry.NewPaginationKeyDataFromRow(this.rows[0], this.table.PaginationKey)
	this.Require().Nil(err)

	builder := ghostferry.DefaultBuildSelect(this.columnsToSelect, this.table, lastPaginationKeyData, 5, false)
	sql, args, err := builder.ToSql()
	this.Require().Nil(err)
	this.Require().Equal(sql, "SELECT * FROM `test_schema`.`test_table` WHERE `col1`>? OR `col1`=? AND `col2`>? OR `col1`=? AND `col2`=? AND `col3`>? ORDER BY `col1`, `col2`, `col3` LIMIT 5")
	this.Require().Equal(args, []interface{}{int64(1), int64(1), "one", int64(1), "one", int64(2)})
}

func (this *CompositePaginationKeyTestSuite) TestDefaultBuildSelectWithResumeDataDescending() {
	lastPaginationKeyData, err := ghostferry.NewPaginationKeyDataFromRow(this.rows[0], this.table.PaginationKey)
	this.Require().Nil(err)

	builder := ghostferry.DefaultBuildSelect(this.columnsToSelect, this.table, lastPaginationKeyData, 5, false)
	sql, args, err := builder.ToSql()
	this.Require().Nil(err)
	this.Require().Equal(sql, "SELECT * FROM `test_schema`.`test_table` WHERE `col1`<? OR `col1`=? AND `col2`<? OR `col1`=? AND `col2`=? AND `col3`<? ORDER BY `col1` DESC, `col2` DESC, `col3` DESC LIMIT 5")
	this.Require().Equal(args, []interface{}{int64(1), int64(1), "one", int64(1), "one", int64(2)})
}

func (this *CompositePaginationKeyTestSuite) TestPaginationKeyDatacompare() {
	data1, err := ghostferry.NewPaginationKeyDataFromRow(this.rows[0], this.table.PaginationKey)
	this.Require().Nil(err)

	data2, err := ghostferry.NewPaginationKeyDataFromRow(this.rows[1], this.table.PaginationKey)
	this.Require().Nil(err)

	this.Require().Equal(data1.Compare(data1), 0)
	this.Require().Equal(data1.Compare(data2), -1)
	this.Require().Equal(data2.Compare(data1), 1)

	var data3 *ghostferry.PaginationKeyData
	this.Require().Equal(data3.Compare(data3), 0)
	this.Require().Equal(data3.Compare(data1), -1)
	this.Require().Equal(data1.Compare(data3), 1)
}

func TestCompositePaginationKey(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, new(CompositePaginationKeyTestSuite))
}
