package test

import (
	"testing"

	"github.com/Shopify/ghostferry"
	"github.com/Shopify/ghostferry/replicatedb"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/suite"

	sqlSchema "github.com/siddontang/go-mysql/schema"
)

type FilterTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite
	tablenames []string
}

func (this *FilterTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	this.tablenames = []string{"test_table_1", "test_table_2", "test_table_3"}
	for _, tablename := range this.tablenames {
		testhelpers.SeedInitialData(this.Ferry.SourceDB, testhelpers.TestSchemaName, tablename, 0)
	}
}

func (this *FilterTestSuite) TestLoadDbs() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		replicatedb.NewStaticDbFilter([]string{"gftest", "gftest2"}),
		nil,
		nil,
		nil,
	)

	this.Require().Nil(err)
	this.Require().Equal(len(this.tablenames), len(tables))
}

func (this *FilterTestSuite) TestLoadDbsFiltered() {
	tables, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		replicatedb.NewStaticDbFilter([]string{"gftest2"}),
		nil,
		nil,
		nil,
	)

	this.Require().Nil(err)
	this.Require().Equal(0, len(tables))
}

func (this *FilterTestSuite) TestFilterWithSimpleList() {
	list := []string{"str1", "str2", "str3"}
	filter := replicatedb.StaticDbFilter{
		Dbs: []string{"str1"},
	}

	this.assertBothFilters([]string{"str1"}, filter, list)
}

func (this *FilterTestSuite) TestFilterWithNonExistentKeyInDbs() {
	list := []string{"str1", "str2", "str3"}
	filter := replicatedb.StaticDbFilter{
		Dbs: []string{"str1", "nonexistent"},
	}

	this.assertBothFilters([]string{"str1"}, filter, list)
}

func (this *FilterTestSuite) TestFilterWithEmptyFilter() {
	list := []string{"str1", "str2", "str3"}
	filter := replicatedb.StaticDbFilter{}
	this.assertBothFilters([]string{}, filter, list)
}

func (this *FilterTestSuite) TestFilterForEmptyList() {
	list := []string{}
	filter := replicatedb.StaticDbFilter{}
	this.assertBothFilters(list, filter, list)
}

func (this *FilterTestSuite) assertBothFilters(expected []string, filter replicatedb.StaticDbFilter, list []string) {
	actualDbs, err := filter.ApplicableDatabases(list)
	this.Require().Nil(err)
	this.Require().Equal(expected, actualDbs)

	var schemas []*ghostferry.TableSchema
	for _, schemaName := range list {
		schemas = append(schemas, &ghostferry.TableSchema{
			Table: &sqlSchema.Table{Schema: schemaName, Name: "some_table"},
		})
	}

	actualTables, err := filter.ApplicableTables(schemas)
	this.Require().Nil(err)

	this.Require().Equal(len(actualTables), len(expected))
}

func TestFilter(t *testing.T) {
	testhelpers.SetupTest()
	suite.Run(t, &FilterTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
